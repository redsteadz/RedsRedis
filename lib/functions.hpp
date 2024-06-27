
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

static struct {
  HMap db;
} g_data;

struct Entry {
  struct HNode node;
  string key;
  string val;
  uint32_t type = 0;
  ZSet *zset = NULL;
};

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
  if (cmd.size() == 6 && cmd[0] == "zquery") {
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
  if (con->state == REQ) {
    HandleReq(con);
  } else if (con->state == RES) {
    HandleRes(con);
  } else {
    assert(0);
  }
}
