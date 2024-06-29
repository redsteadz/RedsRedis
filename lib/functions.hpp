
#include "Zset.h"
#include "hash.h"
#include "structures.hpp"
#include "unordered_map"
#include <arpa/inet.h>
#include <cassert>
#include <cerrno>
#include <cstdio>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <iterator>
#include <netinet/ip.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>
using namespace std;

#pragma once

struct Entry {
  struct HNode node;
  string key;
  string val;
  uint32_t type = 0;
  ZSet *zset = NULL;

  size_t heap_idx;
};

static bool hnode_same(HNode *lhs, HNode *rhs) {
  Entry *l = container_of(lhs, Entry, node);
  Entry *r = container_of(rhs, Entry, node);
  return l->key == r->key;
}

static void entry_set_ttl(Entry *ent, int64_t ttl_ms) {
  if (ttl_ms < 0 && ent->heap_idx != (size_t)-1) {
    // erase an item from the heap
    // by replacing it with the last item in the array.
    size_t pos = ent->heap_idx;
    g_data.heap[pos] = g_data.heap.back();
    g_data.heap.pop_back();
    if (pos < g_data.heap.size()) {
      heap_update(g_data.heap.data(), pos, g_data.heap.size());
    }
    ent->heap_idx = -1;
  } else if (ttl_ms >= 0) {
    size_t pos = ent->heap_idx;
    if (pos == (size_t)-1) {
      // add an new item to the heap
      HeapItem item;
      item.ref = &ent->heap_idx;
      g_data.heap.push_back(item);
      pos = g_data.heap.size() - 1;
    }
    g_data.heap[pos].val = get_monotonic_usec() + (uint64_t)ttl_ms * 1000;
    heap_update(g_data.heap.data(), pos, g_data.heap.size());
  }
}

static void entry_del(Entry *ent) {
  switch (ent->type) {
  case T_ZSET:
    zset_dispose(ent->zset);
    delete ent->zset;
    break;
  }
  entry_set_ttl(ent, -1);
  delete ent;
}

static void process_timers() {
  uint64_t now_us = get_monotonic_usec();
  while (!dlist_empty(&g_data.idle_list)) {
    Connection *next =
        container_of(g_data.idle_list.next, Connection, idle_list);
    uint64_t next_us = next->idle_start + k_idle_timeout_ms * 1000;
    // cout << "now: " << now_us << " next: " << next_us << endl;
    if (next_us >= now_us + 10000) {
      break;
    }

    printf("removing idle connection: %d\n", next->fd);
    conn_done(next);
  }
  const size_t k_max_works = 2000;
  size_t nworks = 0;
  while (!g_data.heap.empty() && g_data.heap[0].val < now_us) {
    Entry *ent = container_of(g_data.heap[0].ref, Entry, heap_idx);
    HNode *node = hm_pop(&g_data.db, &ent->node, &hnode_same);
    assert(node == &ent->node);
    entry_del(ent);
    if (nworks++ >= k_max_works) {
      // don't stall the server if too many keys are expiring at once
      break;
    }
  }
}

static void fd_set_nb(int fd) {
  errno = 0;
  int flags = fcntl(fd, F_GETFL, 0);
  if (errno) {
    perror("fcntl error");
    return;
  }

  flags |= O_NONBLOCK;

  errno = 0;
  (void)fcntl(fd, F_SETFL, flags);
  if (errno) {
    // die("fcntl error");
  }
}

static void connection_make(vector<Connection *> &connections,
                            Connection *con) {
  if ((size_t)con->fd >= connections.size()) {
    connections.resize(con->fd + 1);
  }
  connections[con->fd] = con;
}

static int32_t acceptConnection(int fd, vector<Connection *> &connections) {
  struct sockaddr_in client_addr = {};
  socklen_t client_addr_len = sizeof(client_addr);
  // cout << "waiting for client" << endl;
  int conn_fd = accept(fd, (struct sockaddr *)&client_addr, &client_addr_len);
  if (conn_fd < 0) {
    perror("accept");
  }
  // cout << "Connection made <" << endl;
  fd_set_nb(conn_fd);
  struct Connection *con =
      (struct Connection *)malloc(sizeof(struct Connection));
  if (!con) {
    close(conn_fd);
    return -1;
  }
  con->fd = conn_fd;
  con->state = REQ;
  con->read_size = 0;
  con->write_size = 0;
  con->write_sent = 0;
  con->idle_start = get_monotonic_usec();
  dlist_insert_before(&g_data.idle_list, &con->idle_list);
  connection_make(connections, con);
  cout << "Connection made <" << con->fd << ">" << endl;
  return 0;
}

static bool try_res(Connection *con) {
  ssize_t rv = 0;
  do {
    ssize_t remain = con->write_size - con->write_sent;
    rv = write(con->fd, con->writeBuf + con->write_sent, remain);
  } while (rv < 0 && errno == EINTR);
  if (rv < 0 && errno == EAGAIN) {
    // EAGAIN faced
    return false;
  }
  if (rv < 0) {
    perror("write");
    con->state = END;
    return false;
  }
  con->write_sent += (size_t)rv;
  assert(con->write_sent <= con->write_size);
  if (con->write_sent == con->write_size) {
    con->state = REQ;
    // cout << "Connection: Req" << endl;
    con->write_sent = 0;
    con->write_size = 0;
    return false;
  }
  return true;
}

static void HandleRes(Connection *con) {
  while (try_res(con)) {
  }
}

static unordered_map<string, string> database;

static bool entry_cmp(HNode *lhs, HNode *rhs) {
  struct Entry *le = container_of(lhs, struct Entry, node);
  struct Entry *re = container_of(rhs, struct Entry, node);
  return le->key == re->key && le->type == re->type;
}

static void out_str(string &out, string &val) {
  out.push_back(SER_STR);
  uint32_t len = (uint32_t)val.size();
  out.append((char *)&len, 4);
  out.append(val);
}

static void out_str(string &out, char *val, size_t len) {
  out.push_back(SER_STR);
  out.append((char *)&len, 4);
  out.append(val, len);
}

static void out_int(string &out, int64_t val) {
  out.push_back(SER_INT);
  out.append((char *)&val, 8);
}

static void out_err(string &out, string &val) {
  out.push_back(SER_ERR);
  uint32_t len = (uint32_t)val.size();
  out.append((char *)&len, 4);
  out.append(val);
}

static void out_arr(string &out, uint32_t &size) {
  out.push_back(SER_ARR);
  cout << size << endl;
  out.append((char *)&size, 4);
}

static bool str2int(const std::string &s, int64_t &out) {
  char *endp = NULL;
  out = strtoll(s.c_str(), &endp, 10);
  return endp == s.c_str() + s.size();
}

static bool entry_eq(HNode *lhs, HNode *rhs) {
  // Make sure the entry are the same
  struct Entry *le = container_of(lhs, struct Entry, node);
  struct Entry *re = container_of(rhs, struct Entry, node);
  return le->key == re->key && le->val == re->val;
}

static uint32_t do_expire(std::vector<std::string> &cmd, std::string &out) {
  int64_t ttl_ms = 0;
  if (!str2int(cmd[2], ttl_ms)) {
    string msg = "expect int64";
    out_err(out, msg);
    return RES_ERR;
  }

  Entry key;
  key.key.swap(cmd[1]);
  key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());

  HNode *node = hm_find(&g_data.db, &key.node, &entry_eq);
  if (node) {
    Entry *ent = container_of(node, Entry, node);
    entry_set_ttl(ent, ttl_ms);
  }
  out_int(out, node ? 1 : 0);
  return RES_OK;
}

static void do_ttl(std::vector<std::string> &cmd, std::string &out) {
  Entry key;
  key.key.swap(cmd[1]);
  key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());

  HNode *node = hm_find(&g_data.db, &key.node, &entry_eq);
  if (!node) {
    return out_int(out, -2);
  }

  Entry *ent = container_of(node, Entry, node);
  if (ent->heap_idx == (size_t)-1) {
    return out_int(out, -1);
  }

  uint64_t expire_at = g_data.heap[ent->heap_idx].val;
  uint64_t now_us = get_monotonic_usec();
  return out_int(out, expire_at > now_us ? (expire_at - now_us) / 1000 : 0);
}

static uint32_t do_get(vector<string> &cmd, string &out) {
  Entry key;
  key.key.swap(cmd[1]);
  key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
  HNode *node = hm_find(&g_data.db, &key.node, &entry_cmp);

  if (!node) {
    string msg = "Not found";
    out_err(out, msg);
    return RES_NF;
  }

  string &val = (container_of(node, Entry, node))->val;
  assert(val.size() <= MAX_BUF);
  out_str(out, val);
  return RES_OK;
}

static uint32_t do_del(vector<string> &cmd, string &out) {
  // (void)writeBuf;
  // (void)write_size;
  Entry key;
  key.key.swap(cmd[1]);
  key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());

  out.push_back(SER_INT);
  HNode *node = hm_pop(&g_data.db, &key.node, &entry_cmp);
  uint64_t val = 0;
  if (node) {
    delete container_of(node, Entry, node);
    val = 1;
  }
  out.append((char *)&val, 8);
  return RES_OK;
}

static uint32_t do_set(vector<string> &cmd, string &out) {
  Entry key;
  key.key.swap(cmd[1]);
  key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
  HNode *node = hm_find(&g_data.db, &key.node, &entry_cmp);
  out.push_back(SER_NIL);
  if (node) {
    (container_of(node, Entry, node))->val.swap(cmd[2]);
  } else {
    Entry *e = new Entry(key);
    e->val.swap(cmd[2]);
    hm_insert(&g_data.db, &e->node);
  }

  return RES_OK;
}
// TYPE - SIZE - DATA
static void pack_str(HNode *node, void *container) {
  string &out = *(string *)container;
  out_str(out, (container_of(node, Entry, node))->val);
}
static uint32_t do_keys(vector<string> &cmd, string &out) {
  uint32_t size = (uint32_t)hm_size(&g_data.db);
  out_arr(out, size);
  h_scan(&g_data.db.ht1, pack_str, &out);
  h_scan(&g_data.db.ht2, pack_str, &out);
  return 0;
}

static uint32_t do_zscore(vector<string> &cmd, string &out) {
  string zsetName = cmd[1];
  string member = cmd[2];
  Entry key;
  key.key = zsetName;
  key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
  key.type = 1;
  HNode *node = hm_find(&g_data.db, &key.node, &entry_cmp);
  if (!node) {
    string reply = "Not found";
    out_err(out, reply);
    return RES_NF;
  }

  ZSet *set = (container_of(node, Entry, node))->zset;
  ZNode *znode = zset_lookup(set, member.data(), member.length());
  int score = znode->score;
  out_int(out, score);
  return RES_OK;
}

static uint32_t do_zadd(vector<string> &cmd, string &out) {
  string zsetName = cmd[1];
  double score = atof(cmd[2].c_str());
  Entry key;
  key.key = zsetName;
  key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
  key.type = 1;
  HNode *node = hm_find(&g_data.db, &key.node, &entry_cmp);
  if (!node) {
    Entry *e = new Entry(key);
    e->zset = new ZSet();
    zset_add(e->zset, cmd[3].data(), cmd[3].length(), score);
    hm_insert(&g_data.db, &e->node);
  } else {
    ZSet *set = (container_of(node, Entry, node))->zset;
    zset_add(set, cmd[3].data(), cmd[3].length(), score);
  }
  out.push_back(SER_NIL);
  return RES_OK;
}

static void *begin_arr(string &out) {
  out.push_back(SER_ARR);
  uint32_t placeHolder = 0;
  out.append((char *)&placeHolder, 4);
  return &out;
}

static void end_arr(string &out, void *arr, uint32_t len) {
  char len_arr[4];
  len_arr[0] = (len >> 24) & 0xff;
  len_arr[1] = (len >> 16) & 0xff;
  len_arr[2] = (len >> 8) & 0xff;
  len_arr[3] = len & 0xff;
  for (int i = 1; i < 5; i++) {
    out[i] = len_arr[i - 1];
  }

  out.append((char *)&arr, len);
}

static uint32_t do_zquery(vector<string> &cmd, string &out) {
  string zsetName = cmd[1];
  double score = atof(cmd[2].c_str());
  string name = cmd[3];
  uint64_t offset = stoi(cmd[4]);
  uint32_t limit = stoi(cmd[5]);
  Entry key;
  key.key = zsetName;
  key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
  key.type = 1;
  HNode *node = hm_find(&g_data.db, &key.node, &entry_cmp);
  if (!node) {
    string reply = "Not found";
    out_err(out, reply);
    return RES_NF;
  }
  ZSet *s = (container_of(node, Entry, node))->zset;
  ZNode *znode = zset_query(s, score, name.data(), name.length());
  znode = znode_offset(znode, offset);
  void *arr = begin_arr(out);
  uint32_t n = 0;
  while (znode && n < limit) {
    out_str(out, znode->name, znode->len);
    cout << znode->name << endl;
    cout << znode->score << endl;
    out_int(out, znode->score);
    znode = znode_offset(znode, 1);
    n++;
  }
  end_arr(out, arr, n);
  return RES_OK;
}

static uint32_t try_cmd(vector<string> &cmd, string &out) {
  if (cmd.size() == 2 && cmd[0] == "pttl") {
    do_ttl(cmd, out);
    return RES_OK;
  } else if (cmd.size() == 2 && cmd[0] == "pexpire") {
    return do_expire(cmd, out);
  } else if (cmd.size() == 6 && cmd[0] == "zquery") {
    return do_zquery(cmd, out);
  } else if (cmd.size() == 3 && cmd[0] == "zscore") {
    return do_zscore(cmd, out);
  } else if (cmd.size() == 4 && cmd[0] == "zadd") {
    return do_zadd(cmd, out);
  } else if (cmd.size() > 0 && cmd[0] == "keys") {
    return do_keys(cmd, out);
  } else if (cmd.size() == 3 && cmd[0] == "set") {
    return do_set(cmd, out);
  } else if (cmd.size() == 2 && cmd[0] == "get") {
    return do_get(cmd, out);
  } else if (cmd.size() == 2 && cmd[0] == "del") {
    return do_del(cmd, out);
  }
  string reply = "Error Invalid Command";
  out_err(out, reply);
  return RES_ERR;
}

static bool try_req(Connection *con) {
  // Try evaluating this request
  // cout << "Trying request" << endl;
  // First 4 bytes is for nstr then next is for len of the first string
  // cout << con->read_size << endl;
  if (con->read_size < 4) {
    // Wait for it
    return false;
  }
  uint32_t nstr = 0;
  memcpy(&nstr, &con->readBuf[0], 4);

  int cur = 4;
  int lengths[nstr];
  memset(lengths, 0, sizeof(lengths));
  vector<string> cmd;
  for (int i = 0; i < nstr; i++) {
    // Have you read enough data ?
    // cout << cur << endl;
    if (cur + 4 > con->read_size)
      return false;
    memcpy(&lengths[i], &con->readBuf[cur], 4);
    cur += 4;
    // Have you read enough data ?
    // cout << cur << endl;
    if (cur > con->read_size)
      return false;
    char str[lengths[i] + 1];
    memcpy(str, &con->readBuf[cur], lengths[i]);
    str[lengths[i]] = '\0';
    cmd.push_back(string(str, lengths[i]));
    cur += lengths[i];
  }
  string out;
  // SIZE_OF_BUF _ TYPE _ LENGTH _ DATA
  uint32_t res = try_cmd(cmd, out);
  assert((uint32_t)out.size() <= MAX_BUF);
  uint32_t wlen = (uint32_t)out.size();
  memcpy(&con->writeBuf[0], &wlen, 4);
  memcpy(&con->writeBuf[4], out.data(), out.size());
  con->write_size = wlen + 4;

  size_t rem = con->read_size - cur;
  if (rem) {
    memmove(&con->readBuf, &con->readBuf[cur], rem);
  }
  con->read_size = rem;

  con->state = RES;
  HandleRes(con);
  return (con->state == REQ);
}

static bool fill_buff(Connection *con) {
  // cout << "FILLING " << con->read_size << endl;
  assert(con->read_size < sizeof(con->readBuf));
  ssize_t rv = 0;
  do {
    size_t cap = sizeof(con->readBuf) - con->read_size;
    rv = read(con->fd, &con->readBuf[con->read_size], cap);
  } while (rv < 0 && errno == EINTR);
  // cout << "Read RV: " << rv << endl;
  if (rv < 0 && errno == EAGAIN) {
    // cout << "RV < 0 && errno == EAGAIN" << endl;
    return false;
  }
  if (rv < 0) {
    cout << "read() error" << endl;
    con->state = END;
  }
  if (rv == 0) {
    if (con->read_size > 0)
      cout << "UNEXPECTED ";
    cout << " EOF" << endl;
    con->state = END;
    return false;
  }
  con->read_size += rv;
  assert(con->read_size <= sizeof(con->readBuf));
  // Try requesting now
  // cout << "ReadSize : " << con->read_size << endl;
  while (try_req(con)) {
  }
  return (con->state == REQ);
}

static void HandleReq(Connection *con) {
  // Fill to the max
  while (fill_buff(con)) {
  }
}

static void HandleConnection(Connection *con) {
  // Update the timer in the connection
  con->idle_start = get_monotonic_usec();
  dlist_detach(&con->idle_list);
  dlist_insert_before(&g_data.idle_list, &con->idle_list);

  if (con->state == REQ) {
    HandleReq(con);
  } else if (con->state == RES) {
    HandleRes(con);
  } else {
    assert(0);
  }
}
