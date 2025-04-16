#ifndef FUNCTIONS_H
#define FUNCTIONS_H

#include "Zset.h"
#include "hash.h"
#include "structures.h"
#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/ip.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <time.h>

// Function declarations
bool hnode_same(HNode* lhs, HNode* rhs);
void entry_set_ttl(Entry* ent, int64_t ttl_ms);
void entry_destroy(Entry* ent);
void entry_del_async(void* arg);
void entry_del(Entry* ent);
void process_timers(void);
void fd_set_nb(int fd);
void connection_make(Connection** connections, size_t *size, Connection* con);
int32_t acceptConnection(int fd, Connection** connections, size_t *size);
bool try_res(Connection* con);
void HandleRes(Connection* con);
bool entry_cmp(HNode* lhs, HNode* rhs);
void out_str(char* out, size_t* out_len, const char* val, size_t val_len);
void out_int(char* out, size_t* out_len, int64_t val);
void out_err(char* out, size_t* out_len, const char* val, size_t val_len);
void out_arr(char* out, size_t* out_len, uint32_t size);
bool str2int(const char* s, int64_t* out);
bool entry_eq(HNode* lhs, HNode* rhs);
uint32_t do_expire(char** cmd, size_t cmd_len, char* out, size_t* out_len);
void do_ttl(char** cmd, size_t cmd_len, char* out, size_t* out_len);
uint32_t do_get(char** cmd, size_t cmd_len, char* out, size_t* out_len);
uint32_t do_del(char** cmd, size_t cmd_len, char* out, size_t* out_len);
uint32_t do_set(char** cmd, size_t cmd_len, char* out, size_t* out_len);
void pack_str(HNode* node, void* container);
uint32_t do_keys(char** cmd, size_t cmd_len, char* out, size_t* out_len);
uint32_t do_zscore(char** cmd, size_t cmd_len, char* out, size_t* out_len);
uint32_t do_zadd(char** cmd, size_t cmd_len, char* out, size_t* out_len);
void* begin_arr(char* out, size_t* out_len);
void end_arr(char* out, size_t* out_len, void* arr, uint32_t len);
uint32_t do_zquery(char** cmd, size_t cmd_len, char* out, size_t* out_len);
uint32_t try_cmd(char** cmd, size_t cmd_len, char* out, size_t* out_len);
bool try_req(Connection* con);
bool fill_buff(Connection* con);
void HandleReq(Connection* con);
void HandleConnection(Connection* con);

#endif // FUNCTIONS_H 