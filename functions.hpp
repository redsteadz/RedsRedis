
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
static uint32_t try_cmd(vector<string> &cmd,  uint8_t *writeBuf,
                        size_t &write_size) {
  if (cmd.size() == 3 && cmd[0] == "set") {
    cout << "Setting " << endl;
    database[cmd[1]] = cmd[2];
    char reply[] = "Written"; 
    size_t len = (size_t)(strlen(reply));
    memcpy(writeBuf, &reply, len);
    write_size = len;
    return 100;
  } else if (cmd.size() == 2 && cmd[0] == "get") {
    if (database.find(cmd[1]) == database.end())
      return RES_NF;
    string &reply = database[cmd[1]];
    size_t len = (size_t)reply.length();
    memcpy(writeBuf, reply.data(), len);
    memcpy(&write_size, &len, 4);
    return RES_OK;
    // cout << "Got reply " << reply << endl;
  } else if (cmd[0] == "del") {
    if (database.find(cmd[1]) == database.end())
      return RES_NF;
    database.erase(cmd[1]);
    return RES_OK;
  } else {
    const char *reply = "Error Invalid Command";
    size_t len = (size_t)strlen(reply);
    memcpy(writeBuf, reply, len);
    memcpy(&write_size, &len, 4);
    return RES_ERR;
  }
}

static bool try_req(Connection *con) {
  // Try evaluating this request
  cout << "Trying request" << endl;
  // First 4 bytes is for nstr then next is for len of the first string
  cout << con->read_size << endl;
  if (con->read_size < 4) {
    // Wait for it
    return false;
  }
  uint32_t nstr = 0;
  memcpy(&nstr, &con->readBuf[0], 4);

  int cur = 4;
  int lengths[4] = {0};
  vector<string> cmd;
  for (int i = 0; i < nstr; i++) {
    // Have you read enough data ?
    cout << cur << endl;
    if (cur + 4 > con->read_size)
      return false;
    memcpy(&lengths[i], &con->readBuf[cur], 4);
    cur += 4;
    // Have you read enough data ?
    cout << cur << endl;
    if (cur > con->read_size)
      return false;
    char str[lengths[i] + 1];
    memcpy(str, &con->readBuf[cur], lengths[i]);
    str[lengths[i]] = '\0';
    cmd.push_back(string(str, lengths[i]));
    cur += lengths[i];
  }
  for (string s : cmd) {
    cout << s << " ";
  }
  cout << endl;

  uint32_t res = try_cmd(cmd, &con->writeBuf[4 + 4], con->write_size);
  memcpy(&con->writeBuf, &res, 4);
  uint32_t len = con->write_size;
  memcpy(&con->writeBuf[4], &len, 4);
  con->write_size += 8;
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
  assert(con->read_size < size(con->readBuf));
  ssize_t rv = 0;
  do {
    size_t cap = sizeof(con->readBuf) - con->read_size;
    rv = read(con->fd, &con->readBuf[con->read_size], cap);
  } while (rv < 0 && errno == EINTR);
  cout << "Read RV: " << rv << endl;
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