#include "dlist.h"
#include "thread.h"
#include "hash.h"
#include "heap.h"
#include <arpa/inet.h>
#include <ctime>
#include <fcntl.h>
#include <iostream>
#include <netinet/ip.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#pragma once
const size_t MAX_BUF = 4096;

enum {
  REQ = 0,
  RES = 1,
  END = 2,
};

enum RES_CODE { RES_OK = 0, RES_ERR = 1, RES_NF = 2 };

enum Type {
  SER_NIL = 0,
  SER_ERR = 1,
  SER_STR = 2,
  SER_INT = 3,
  SER_ARR = 4,
};

enum { T_STR = 0, T_ZSET = 1 };

struct Connection {
  int fd = -1;
  uint32_t state = 0;
  size_t read_size = 0;
  uint8_t readBuf[4 + MAX_BUF];
  size_t write_size = 0;
  size_t write_sent = 0;
  uint8_t writeBuf[4 + MAX_BUF];

  uint64_t idle_start = 0;
  Dlist idle_list;
};

static struct {
  HMap db;
  // Connections in the database
  vector<Connection *> connections;
  Dlist idle_list;
  vector<HeapItem> heap;
  ThreadPool tp;
} g_data;

static uint64_t str_hash(const uint8_t *data, size_t len) {
  uint32_t h = 0x811C9DC5;
  for (size_t i = 0; i < len; i++) {
    h = (h + data[i]) * 0x01000193;
  }
  return h;
}

const uint64_t k_idle_timeout_ms = 30 * 1000;

static uint64_t get_monotonic_usec() {
  timespec tv = {0, 0};
  clock_gettime(CLOCK_MONOTONIC, &tv);
  return uint64_t(tv.tv_sec) * 1000000 + tv.tv_nsec / 1000;
}

static void conn_done(Connection *conn) {
  g_data.connections[conn->fd] = NULL;
  (void)close(conn->fd);
  dlist_detach(&conn->idle_list);
  free(conn);
}

static uint32_t next_timer_ms() {
  uint64_t now_us = get_monotonic_usec();
  uint64_t next_us = (uint64_t)-1;

  // idle timers
  if (!dlist_empty(&g_data.idle_list)) {
    Connection *next =
        container_of(g_data.idle_list.next, Connection, idle_list);
    next_us = next->idle_start + k_idle_timeout_ms * 1000;
  }

  // ttl timers
  if (!g_data.heap.empty() && g_data.heap[0].val < next_us) {
    next_us = g_data.heap[0].val;
  }

  if (next_us == (uint64_t)-1) {
    return 10000; // no timer, the value doesn't matter
  }

  if (next_us <= now_us) {
    // missed?
    return 0;
  }
  return (uint32_t)((next_us - now_us) / 1000);
}
