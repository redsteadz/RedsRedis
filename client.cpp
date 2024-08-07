#include "lib/structures.hpp"
#include <arpa/inet.h>
#include <cassert>
#include <codecvt>
#include <errno.h>
#include <iostream>
#include <netinet/ip.h>
#include <sstream>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>
using namespace std;

static int32_t writeAll(int fd, char *buf, size_t n) {
  while (n > 0) {
    ssize_t writeSize = write(fd, buf, n);
    if (writeSize <= 0) {
      return -1; // Should use throw
    }
    assert((size_t)writeSize <= n);
    n -= (size_t)writeSize;
    buf += writeSize;
  }

  return 0;
}

struct Packet {
  uint32_t len;
  vector<char> msg;
};

Packet parseData(char buff[MAX_BUF]) {
  uint32_t len = 0;
  memcpy(&len, buff, 4);
  // cout << "The len of the received data is: " << len << endl;
  vector<char> msg(buff + 4, buff + 4 + len + 1);
  msg[4 + len] = '\0';
  return {len, msg};
}

static int32_t readIO(int fd, Packet *packet) {
  char buff[MAX_BUF] = {};
  int32_t n = MAX_BUF;
  cout << "READING" << endl;
  ssize_t rs = read(fd, buff, n);
  if (rs <= 0) {
    return -1;
  }
  // cout << "RS: " << rs << endl;
  // cout << "READING" << endl;
  n -= rs;
  // Use this data to built a packet
  // cout << "PARSING DATA" << endl;
  *packet = parseData(buff);
  return 0;
}

static int32_t readAll(int fd, char *buf, size_t n) {
  while (n > 0) {
    ssize_t readSize = read(fd, buf, n);
    if (readSize <= 0) {
      return -1; // Should use throw
    }
    assert((size_t)readSize <= n);
    n -= (size_t)readSize;
    buf += readSize;
  }

  return 0;
}

static int32_t query(int fd, const char *text) {
  uint32_t messageLen = (uint32_t)strlen(text);
  char writeBuff[4 + messageLen];
  memcpy(writeBuff, &messageLen, 4);
  memcpy(writeBuff + 4, text, messageLen);
  if (int32_t err = writeAll(fd, writeBuff, 4 + messageLen) < 0) {
    return err;
  }
  char readBuff[4 + MAX_BUF + 1];
  errno = 0;
  Packet readPacket;
  errno = 0;
  int32_t err = readIO(fd, &readPacket);
  if (err) {
    if (errno == 0) {
      cout << "EOF" << endl;
    } else {
      cout << "read () error" << endl;
    }
    return err;
  }
  // cout << "len: " << readPacket.len << endl;
  cout << "Server Says: " << readPacket.msg.data() << endl;
  return 0;
}

static int32_t read_full(int fd, char *buf, size_t n) {
  while (n > 0) {
    ssize_t rv = read(fd, buf, n);
    if (rv <= 0) {
      return -1; // error, or unexpected EOF
    }
    assert((size_t)rv <= n);
    n -= (size_t)rv;
    buf += rv;
  }
  // cout << "Read Res Length" << endl;
  return 0;
}

uint32_t out_resp(char *rbuf, int32_t size) {
  Type resType = (Type)rbuf[0];
  string resp;
  uint32_t len;
  switch (resType) {
  case Type::SER_STR:
  case Type::SER_ERR:
    if (size < 1 + 4) {
      cout << "Bad Response" << endl;
      return -1;
      break;
    }
    memcpy(&len, &rbuf[1], 4);
    resp.append(&rbuf[5], len);
    printf("[str] - len: %d, res: %s\n", size, resp.c_str());
    return 4 + len + 1;
    break;
  case Type::SER_NIL:
    printf("[nil] - len: %d, res: nil\n", size);
    return 1;
  case Type::SER_INT:
    if (size < 1 + 8) {
      cout << "Bad Response" << endl;
      return -1;
    }
    uint64_t val;
    memcpy(&val, &rbuf[1], 8);
    printf("[int] - len: %d, res: %ld\n", size, val);
    return 8 + 1;
  case Type::SER_ARR:
    if (size < 1 + 4) {
      cout << "Bad Response" << endl;
      return -1;
    }
    // Every element of the array is TLD (Type Value Data)
    memcpy(&len, &rbuf[1], 4);
    uint32_t loc = 4 + 1;
    for (int i = 0; i < len; i++) {
      uint32_t rv = out_resp(&rbuf[loc], size - loc);
      if (rv < 0) {
        return rv;
      }
      loc += rv;
    }

    return loc;
  }
  return 0;
}

static int32_t read_res(int fd) {
  // 4 bytes header
  char rbuf[4 + MAX_BUF];
  errno = 0;
  int32_t err = read_full(fd, rbuf, 4);
  if (err) {
    if (errno == 0) {
      cout << "EOF" << endl;
    } else {
      cout << "read () error" << endl;
    }
    return err;
  }

  uint32_t size = 0;
  memcpy(&size, &rbuf, 4); // assume little endian

  // SIZE_OF_BUF _ TYPE _ LENGTH _ DATA
  errno = 0;
  err = read_full(fd, &rbuf[4], size);
  if (err) {
    if (errno == 0) {
      cout << "EOF" << endl;
    } else {
      cout << "read () error" << endl;
    }
    return err;
  }
  return out_resp(&rbuf[4], size);
}

static int32_t sendReq(int fd, int32_t nstr, char **cmd) {
  // uint32_t messageLen = (uint32_t)strlen(text);
  // char writeBuff[4 + messageLen];
  // memcpy(writeBuff, &messageLen, 4);
  // memcpy(writeBuff + 4, text, messageLen);
  uint32_t packetSize = 4;
  for (int i = 0; i < nstr; i++)
    packetSize += strlen(cmd[i]);
  packetSize += 4 * nstr;
  char packet[packetSize];
  memcpy(&packet, &nstr, 4);
  int cur = 4;
  for (int i = 0; i < nstr; i++) {
    int l = strlen(cmd[i]);
    memcpy(&packet[cur], &l, 4);
    cur += 4;
    memcpy(&packet[cur], cmd[i], l);
    cur += l;
  }
  // cout << "PACKETSIZE: " << packetSize << endl;
  return writeAll(fd, packet, packetSize);
}

uint32_t query(int fd) {
  vector<string> cmd;
  string line;
  getline(cin, line);
  // cout << "THIS IS A LINE : " << line << endl;
  istringstream l(line);
  string word;
  int size = 0;
  while (l >> word) {
    cmd.push_back(word);
    // cout << word << endl;
    size += word.length();
  }
  if (cmd.size() > 0) {
    char *cmd_chr[cmd.size()];
    int loc = 0;
    for (string words : cmd) {
      cmd_chr[loc] = new char[words.size() + 1];
      strcpy(cmd_chr[loc++], words.c_str());
    }
    // query(fd, cmd_chr);
    for (int i = 0; i < cmd.size(); i++) {
      // cout << cmd_chr[i] << " ";
    }
    // cout << endl;
    // cout << "QUERY: " << line << endl;
    int32_t err = sendReq(fd, cmd.size(), cmd_chr);
    if (err) {
      cout << "Req error" << endl;
      return err;
    }
    err = read_res(fd);
    if (err < 0) {
      cout << "Recv error" << endl;
      return err;
    }
  }

  return 1;
}

int main(int argc, char *argv[]) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    perror("socket");
    return 1;
  }

  struct sockaddr_in addr;
  addr.sin_family = AF_INET;
  addr.sin_port = htons(1800);
  addr.sin_addr.s_addr = inet_addr("127.0.0.1");
  // fd_set_nb(fd);
  if (connect(fd, (const sockaddr *)&addr, sizeof(addr)) < 0) {
    perror("connect");
    close(fd);
    return 1;
  }
  while (query(fd)) {
  }
  shutdown(fd, SHUT_RDWR);
  close(fd);
  return 0;
}
