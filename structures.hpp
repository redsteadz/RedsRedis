#include <arpa/inet.h>
#include <fcntl.h>
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

struct Connection {
  int fd = -1;
  uint32_t state = 0;
  size_t read_size = 0;
  uint8_t readBuf[4 + MAX_BUF];
  size_t write_size = 0;
  size_t write_sent = 0;
  uint8_t writeBuf[4 + MAX_BUF];
};
