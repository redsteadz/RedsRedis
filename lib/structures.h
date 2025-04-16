#ifndef STRUCTURES_H
#define STRUCTURES_H

#define _POSIX_C_SOURCE 200809L

#include "dlist.h"
#include "thread.h"
#include "hash.h"
#include "heap.h"
#include <arpa/inet.h>
// #include <ctime>
#include <fcntl.h>
// #include <iostream>
#include <netinet/ip.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <time.h>
#include <stdbool.h>
#include <stddef.h>
#include "Zset.h"

#define MAX_BUF 4096
#define MAX_CONNECTIONS 1024
#define MAX_HEAP_ITEMS 1024

// Message types and Connection states
typedef enum {
    REQ = 0,
    RES = 1,
    END = 2,
} ConnState;

// Response codes
enum RES_CODE { 
    RES_OK = 0, 
    RES_ERR = 1, 
    RES_NF = 2 
};

// Serialization types
enum Type {
    SER_NIL = 0,
    SER_ERR = 1,
    SER_STR = 2,
    SER_INT = 3,
    SER_ARR = 4,
};

// Data types
typedef enum { 
    T_STR = 0, 
    T_ZSET = 1 
} EntryType;

// Connection structure
typedef struct Connection {
    int fd;
    ConnState state;
    char readBuf[4096];
    size_t read_size;
    char writeBuf[4096];
    size_t write_size;
    size_t write_sent;
    uint64_t idle_start;
    Dlist idle_list;
} Connection;

// Entry structure
typedef struct Entry {
    HNode node;
    EntryType type;
    char* key;
    char* val;
    size_t heap_idx;
    ZSet* zset;
} Entry;

// Global data structure
typedef struct GlobalData {
    HMap db;
    Connection* connections[10240];
    size_t connections_size;
    Dlist idle_list;
    HeapItem heap[MAX_HEAP_ITEMS];
    size_t heap_size;
    ThreadPool tp;
} GlobalData;

// Global data instance
extern GlobalData g_data;

// Function declarations
uint64_t str_hash(const uint8_t* data, size_t len);
uint64_t get_monotonic_usec(void);
void conn_done(Connection* conn);
uint64_t next_timer_ms(void);

#endif // STRUCTURES_H
