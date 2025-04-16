#include "functions.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

// Global data instance
extern GlobalData g_data;

// Compare two hash nodes
bool hnode_same(HNode* lhs, HNode* rhs) {
    return lhs == rhs;
}

// Set TTL for an entry
void entry_set_ttl(Entry* ent, int64_t ttl_ms) {
    if (ttl_ms < 0 && ent->heap_idx != (size_t)-1) {
        // Erase an item from the heap by replacing it with the last item
        size_t pos = ent->heap_idx;
        g_data.heap[pos] = g_data.heap[g_data.heap_size - 1];
        g_data.heap_size--;
        if (pos < g_data.heap_size) {
            heap_update(g_data.heap, pos, g_data.heap_size);
        }
        ent->heap_idx = (size_t)-1;
    } else if (ttl_ms >= 0) {
        size_t pos = ent->heap_idx;
        if (pos == (size_t)-1) {
            // Add a new item to the heap
            if (g_data.heap_size >= MAX_HEAP_ITEMS) {
                return; // Heap is full
            }
            HeapItem item = {0};
            item.ref = &ent->heap_idx;
            g_data.heap[g_data.heap_size] = item;
            pos = g_data.heap_size;
            g_data.heap_size++;
        }
        g_data.heap[pos].val = get_monotonic_usec() + (uint64_t)ttl_ms * 1000;
        heap_update(g_data.heap, pos, g_data.heap_size);
    }
}

// Destroy an entry
void entry_destroy(Entry* ent) {
    switch (ent->type) {
        case T_ZSET:
            if (ent->zset) {
                zset_dispose(ent->zset);
                free(ent->zset);
            }
            break;
    }
    free(ent->key);
    free(ent->val);
    free(ent);
}

// Asynchronous entry deletion
void entry_del_async(void* arg) {
    entry_destroy((Entry*)arg);
}

// Delete an entry
void entry_del(Entry* ent) {
    entry_set_ttl(ent, -1);

    const size_t k_large_container_size = 10000;
    bool too_big = false;
    switch (ent->type) {
        case T_ZSET:
            too_big = hm_size(&ent->zset->db) > k_large_container_size;
            break;
    }

    if (too_big) {
        thread_pool_queue(&g_data.tp, entry_del_async, ent);
    } else {
        entry_destroy(ent);
    }
}
#define k_idle_timeout_ms 30000
// Process timers
void process_timers(void) {
    uint64_t now_us = get_monotonic_usec();
    
    // Process idle connections
    while (!dlist_empty(&g_data.idle_list)) {
        Connection* next = container_of(g_data.idle_list.next, Connection, idle_list);
        uint64_t next_us = next->idle_start + k_idle_timeout_ms * 1000;
        if (next_us >= now_us + 10000) {
            break;
        }

        printf("removing idle connection: %d\n", next->fd);
        conn_done(next);
    }
    
    // Process TTL timers
    const size_t k_max_works = 2000;
    size_t nworks = 0;
    while (g_data.heap_size > 0 && g_data.heap[0].val < now_us) {
        Entry* ent = container_of(g_data.heap[0].ref, Entry, heap_idx);
        printf("Expiring key: %s\n", ent->key);
        HNode* node = hm_pop(&g_data.db, &ent->node, hnode_same);
        assert(node == &ent->node);
        entry_del(ent);
        if (nworks++ >= k_max_works) {
            // Don't stall the server if too many keys are expiring at once
            break;
        }
    }
}

// Set socket to non-blocking mode
void fd_set_nb(int fd) {
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
        perror("fcntl error");
    }
}

// Add connection to connections array
void connection_make(Connection** connections, size_t *size, Connection* con) {
    // if (size < (size_t)con->fd) {
    //     connections = (Connection**)realloc(connections, con->fd + 1);
    //     size = con->fd + 1;
    // }

    if (con->fd >= 0 && (size_t)con->fd < size) {
        connections[con->fd] = con;
    }
}

// Accept a new connection
int32_t acceptConnection(int fd, Connection** connections, size_t *size) {
    struct sockaddr_in client_addr = {0};
    socklen_t client_addr_len = sizeof(client_addr);
    
    int conn_fd = accept(fd, (struct sockaddr*)&client_addr, &client_addr_len);
    if (conn_fd < 0) {
        perror("accept");
        return -1;
    }
    
    fd_set_nb(conn_fd);
    Connection* con = (Connection*)malloc(sizeof(Connection));
    if (!con) {
        close(conn_fd);
        return -1;
    }
    
    memset(con, 0, sizeof(Connection));
    con->fd = conn_fd;
    con->state = REQ;
    con->idle_start = get_monotonic_usec();
    dlist_insert_before(&g_data.idle_list, &con->idle_list);
    connection_make(connections, size, con);
    *size += 1;
    printf("size: %zu\n", *size);
    printf("Connection made <%d>\n", con->fd);
    return 0;
}

// Try to send response
bool try_res(Connection* con) {
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
        con->write_sent = 0;
        con->write_size = 0;
        return false;
    }
    
    return true;
}

// Handle response
void HandleRes(Connection* con) {
    while (try_res(con)) {
        // Continue until we can't write more
    }
}

// Compare two entries
bool entry_cmp(HNode* lhs, HNode* rhs) {
    Entry* le = container_of(lhs, Entry, node);
    Entry* re = container_of(rhs, Entry, node);
    return strcmp(le->key, re->key) == 0 && le->type == re->type;
}

// Output string to buffer
void out_str(char* out, size_t* out_len, const char* val, size_t val_len) {
    out[*out_len] = SER_STR;
    (*out_len)++;
    
    memcpy(out + *out_len, &val_len, 4);
    *out_len += 4;
    
    memcpy(out + *out_len, val, val_len);
    *out_len += val_len;
}

// Output integer to buffer
void out_int(char* out, size_t* out_len, int64_t val) {
    out[*out_len] = SER_INT;
    (*out_len)++;
    
    memcpy(out + *out_len, &val, 8);
    *out_len += 8;
}

// Output error to buffer
void out_err(char* out, size_t* out_len, const char* val, size_t val_len) {
    out[*out_len] = SER_ERR;
    (*out_len)++;
    
    memcpy(out + *out_len, &val_len, 4);
    *out_len += 4;
    
    memcpy(out + *out_len, val, val_len);
    *out_len += val_len;
}

// Output array header to buffer
void out_arr(char* out, size_t* out_len, uint32_t size) {
    out[*out_len] = SER_ARR;
    (*out_len)++;
    
    memcpy(out + *out_len, &size, 4);
    *out_len += 4;
}

// Convert string to integer
bool str2int(const char* s, int64_t* out) {
    char* endp = NULL;
    *out = strtoll(s, &endp, 10);
    return endp == s + strlen(s);
}

// Compare two entries for equality
bool entry_eq(HNode* lhs, HNode* rhs) {
    Entry* le = container_of(lhs, Entry, node);
    Entry* re = container_of(rhs, Entry, node);
    return strcmp(le->key, re->key) == 0;
}

// Handle EXPIRE command
uint32_t do_expire(char** cmd, size_t cmd_len, char* out, size_t* out_len) {
    if (cmd_len < 3) {
        const char* msg = "expect int64";
        out_err(out, out_len, msg, strlen(msg));
        return RES_ERR;
    }
    
    int64_t ttl_ms = 0;
    if (!str2int(cmd[2], &ttl_ms)) {
        const char* msg = "expect int64";
        out_err(out, out_len, msg, strlen(msg));
        return RES_ERR;
    }
    
    Entry key = {0};
    key.key = strdup(cmd[1]);
    key.node.hcode = str_hash((uint8_t*)key.key, strlen(key.key));
    
    HNode* node = hm_find(&g_data.db, &key.node, entry_eq);
    if (node) {
        Entry* ent = container_of(node, Entry, node);
        entry_set_ttl(ent, ttl_ms);
    }
    
    out_int(out, out_len, node ? 1 : 0);
    free(key.key);
    return RES_OK;
}

// Handle TTL command
void do_ttl(char** cmd, size_t cmd_len, char* out, size_t* out_len) {
    if (cmd_len < 2) {
        return;
    }
    
    Entry key = {0};
    key.key = strdup(cmd[1]);
    key.node.hcode = str_hash((uint8_t*)key.key, strlen(key.key));
    
    HNode* node = hm_find(&g_data.db, &key.node, entry_eq);
    if (!node) {
        out_int(out, out_len, -2);
        free(key.key);
        return;
    }
    
    Entry* ent = container_of(node, Entry, node);
    if (ent->heap_idx == (size_t)-1) {
        out_int(out, out_len, -1);
        free(key.key);
        return;
    }
    
    uint64_t expire_at = g_data.heap[ent->heap_idx].val;
    uint64_t now_us = get_monotonic_usec();
    out_int(out, out_len, expire_at > now_us ? (expire_at - now_us) / 1000 : 0);
    free(key.key);
}

// Handle GET command
uint32_t do_get(char** cmd, size_t cmd_len, char* out, size_t* out_len) {
    if (cmd_len < 2) {
        const char* msg = "Not found";
        out_err(out, out_len, msg, strlen(msg));
        return RES_NF;
    }
    
    Entry key = {0};
    key.key = strdup(cmd[1]);
    key.node.hcode = str_hash((uint8_t*)key.key, strlen(key.key));
    
    HNode* node = hm_find(&g_data.db, &key.node, entry_cmp);
    if (!node) {
        const char* msg = "Not found";
        out_err(out, out_len, msg, strlen(msg));
        free(key.key);
        return RES_NF;
    }
    
    Entry* ent = container_of(node, Entry, node);
    assert(strlen(ent->val) <= MAX_BUF);
    out_str(out, out_len, ent->val, strlen(ent->val));
    free(key.key);
    return RES_OK;
}

// Handle DEL command
uint32_t do_del(char** cmd, size_t cmd_len, char* out, size_t* out_len) {
    if (cmd_len < 2) {
        out_int(out, out_len, 0);
        return RES_OK;
    }
    
    Entry key = {0};
    key.key = strdup(cmd[1]);
    key.node.hcode = str_hash((uint8_t*)key.key, strlen(key.key));
    
    HNode* node = hm_pop(&g_data.db, &key.node, entry_cmp);
    uint64_t val = 0;
    if (node) {
        Entry* ent = container_of(node, Entry, node);
        entry_destroy(ent);
        val = 1;
    }
    
    out_int(out, out_len, val);
    free(key.key);
    return RES_OK;
}

// Handle SET command
uint32_t do_set(char** cmd, size_t cmd_len, char* out, size_t* out_len) {
    if (cmd_len < 3) {
        const char* msg = "Invalid command";
        out_err(out, out_len, msg, strlen(msg));
        return RES_ERR;
    }
    
    Entry key = {0};
    key.key = strdup(cmd[1]);
    key.node.hcode = str_hash((uint8_t*)key.key, strlen(key.key));
    
    HNode* node = hm_find(&g_data.db, &key.node, entry_cmp);
    out[*out_len] = SER_NIL;
    (*out_len)++;
    
    if (node) {
        Entry* ent = container_of(node, Entry, node);
        free(ent->val);
        ent->val = strdup(cmd[2]);
    } else {
        Entry* e = (Entry*)malloc(sizeof(Entry));
        memset(e, 0, sizeof(Entry));
        e->key = strdup(cmd[1]);
        e->val = strdup(cmd[2]);
        e->type = T_STR;
        e->node.hcode = key.node.hcode;
        hm_insert(&g_data.db, &e->node);
    }
    
    free(key.key);
    return RES_OK;
}

// Pack string for scanning
void pack_str(HNode* node, void* container) {
    char** out = (char**)container;
    size_t* out_len = (size_t*)(container + sizeof(char*));
    Entry* ent = container_of(node, Entry, node);
    out_str(*out, out_len, ent->val, strlen(ent->val));
}

// Handle KEYS command
uint32_t do_keys(char** cmd, size_t cmd_len, char* out, size_t* out_len) {
    uint32_t size = (uint32_t)hm_size(&g_data.db);
    out_arr(out, out_len, size);
    
    char* out_ptr = out + *out_len;
    size_t out_len_ptr = 0;
    void* container[2] = {&out_ptr, &out_len_ptr};
    
    h_scan(&g_data.db.ht1, pack_str, container);
    h_scan(&g_data.db.ht2, pack_str, container);
    
    *out_len += out_len_ptr;
    return RES_OK;
}

// Handle ZSCORE command
uint32_t do_zscore(char** cmd, size_t cmd_len, char* out, size_t* out_len) {
    if (cmd_len < 3) {
        const char* msg = "Invalid command";
        out_err(out, out_len, msg, strlen(msg));
        return RES_ERR;
    }
    
    Entry key = {0};
    key.key = strdup(cmd[1]);
    key.node.hcode = str_hash((uint8_t*)key.key, strlen(key.key));
    key.type = T_ZSET;
    
    HNode* node = hm_find(&g_data.db, &key.node, entry_cmp);
    if (!node) {
        const char* msg = "Not found";
        out_err(out, out_len, msg, strlen(msg));
        free(key.key);
        return RES_NF;
    }
    
    Entry* ent = container_of(node, Entry, node);
    ZNode* znode = zset_lookup(ent->zset, cmd[2], strlen(cmd[2]));
    if (!znode) {
        const char* msg = "Member not found";
        out_err(out, out_len, msg, strlen(msg));
        free(key.key);
        return RES_NF;
    }
    
    out_int(out, out_len, znode->score);
    free(key.key);
    return RES_OK;
}

// Handle ZADD command
uint32_t do_zadd(char** cmd, size_t cmd_len, char* out, size_t* out_len) {
    if (cmd_len < 4) {
        const char* msg = "Invalid command";
        out_err(out, out_len, msg, strlen(msg));
        return RES_ERR;
    }
    
    Entry key = {0};
    key.key = strdup(cmd[1]);
    key.node.hcode = str_hash((uint8_t*)key.key, strlen(key.key));
    key.type = T_ZSET;
    
    HNode* node = hm_find(&g_data.db, &key.node, entry_cmp);
    if (!node) {
        Entry* e = (Entry*)malloc(sizeof(Entry));
        memset(e, 0, sizeof(Entry));
        e->key = strdup(cmd[1]);
        e->type = T_ZSET;
        e->node.hcode = key.node.hcode;
        e->zset = (ZSet*)malloc(sizeof(ZSet));
        memset(e->zset, 0, sizeof(ZSet));
        zset_add(e->zset, cmd[3], strlen(cmd[3]), atof(cmd[2]));
        hm_insert(&g_data.db, &e->node);
    } else {
        Entry* ent = container_of(node, Entry, node);
        zset_add(ent->zset, cmd[3], strlen(cmd[3]), atof(cmd[2]));
    }
    
    out[*out_len] = SER_NIL;
    (*out_len)++;
    free(key.key);
    return RES_OK;
}

// Begin array output
void* begin_arr(char* out, size_t* out_len) {
    out[*out_len] = SER_ARR;
    (*out_len)++;
    
    // Reserve space for length
    size_t len_pos = *out_len;
    *out_len += 4;
    
    return (void*)(out + len_pos);
}

// End array output
void end_arr(char* out, size_t* out_len, void* arr, uint32_t len) {
    char* len_pos = (char*)arr;
    memcpy(len_pos, &len, 4);
}

// Handle ZQUERY command
uint32_t do_zquery(char** cmd, size_t cmd_len, char* out, size_t* out_len) {
    if (cmd_len < 6) {
        const char* msg = "Invalid command";
        out_err(out, out_len, msg, strlen(msg));
        return RES_ERR;
    }
    
    Entry key = {0};
    key.key = strdup(cmd[1]);
    key.node.hcode = str_hash((uint8_t*)key.key, strlen(key.key));
    key.type = T_ZSET;
    
    HNode* node = hm_find(&g_data.db, &key.node, entry_cmp);
    if (!node) {
        const char* msg = "Not found";
        out_err(out, out_len, msg, strlen(msg));
        free(key.key);
        return RES_NF;
    }
    
    Entry* ent = container_of(node, Entry, node);
    double score = atof(cmd[2]);
    int64_t offset = atol(cmd[4]);
    int64_t limit = atol(cmd[5]);
    
    ZNode* znode = zset_query(ent->zset, score, cmd[3], strlen(cmd[3]));
    if (offset != 0) {
        znode = znode_offset(znode, offset);
    }
    
    void* arr = begin_arr(out, out_len);
    uint32_t n = 0;
    
    while (znode && n < limit) {
        out_str(out, out_len, znode->name, znode->len);
        out_int(out, out_len, znode->score);
        znode = znode_offset(znode, 1);
        n++;
    }
    
    end_arr(out, out_len, arr, n);
    free(key.key);
    return RES_OK;
}

// Try to execute a command
uint32_t try_cmd(char** cmd, size_t cmd_len, char* out, size_t* out_len) {
    if (cmd_len == 0) {
        const char* msg = "Empty command";
        out_err(out, out_len, msg, strlen(msg));
        return RES_ERR;
    }
    
    if (strcmp(cmd[0], "pttl") == 0 && cmd_len == 2) {
        do_ttl(cmd, cmd_len, out, out_len);
        return RES_OK;
    } else if (strcmp(cmd[0], "pexpire") == 0 && cmd_len == 3) {
        return do_expire(cmd, cmd_len, out, out_len);
    } else if (strcmp(cmd[0], "zquery") == 0 && cmd_len == 6) {
        return do_zquery(cmd, cmd_len, out, out_len);
    } else if (strcmp(cmd[0], "zscore") == 0 && cmd_len == 3) {
        return do_zscore(cmd, cmd_len, out, out_len);
    } else if (strcmp(cmd[0], "zadd") == 0 && cmd_len == 4) {
        return do_zadd(cmd, cmd_len, out, out_len);
    } else if (strcmp(cmd[0], "keys") == 0) {
        return do_keys(cmd, cmd_len, out, out_len);
    } else if (strcmp(cmd[0], "set") == 0 && cmd_len == 3) {
        return do_set(cmd, cmd_len, out, out_len);
    } else if (strcmp(cmd[0], "get") == 0 && cmd_len == 2) {
        return do_get(cmd, cmd_len, out, out_len);
    } else if (strcmp(cmd[0], "del") == 0 && cmd_len == 2) {
        return do_del(cmd, cmd_len, out, out_len);
    }
    
    const char* msg = "Error Invalid Command";
    out_err(out, out_len, msg, strlen(msg));
    return RES_ERR;
}

// Try to process a request
bool try_req(Connection* con) {
    if (con->read_size < 4) {
        // Wait for it
        return false;
    }
    
    uint32_t nstr = 0;
    memcpy(&nstr, &con->readBuf[0], 4);
    
    int cur = 4;
    int* lengths = (int*)malloc(nstr * sizeof(int));
    if (!lengths) {
        con->state = END;
        return false;
    }
    
    memset(lengths, 0, nstr * sizeof(int));
    char** cmd = (char**)malloc(nstr * sizeof(char*));
    if (!cmd) {
        free(lengths);
        con->state = END;
        return false;
    }
    
    memset(cmd, 0, nstr * sizeof(char*));
    
    for (uint32_t i = 0; i < nstr; i++) {
        if (cur + 4 > con->read_size) {
            // Not enough data
            for (uint32_t j = 0; j < i; j++) {
                free(cmd[j]);
            }
            free(cmd);
            free(lengths);
            return false;
        }
        
        memcpy(&lengths[i], &con->readBuf[cur], 4);
        cur += 4;
        
        if (cur + lengths[i] > con->read_size) {
            // Not enough data
            for (uint32_t j = 0; j < i; j++) {
                free(cmd[j]);
            }
            free(cmd);
            free(lengths);
            return false;
        }
        
        cmd[i] = (char*)malloc(lengths[i] + 1);
        if (!cmd[i]) {
            for (uint32_t j = 0; j < i; j++) {
                free(cmd[j]);
            }
            free(cmd);
            free(lengths);
            con->state = END;
            return false;
        }
        
        memcpy(cmd[i], &con->readBuf[cur], lengths[i]);
        cmd[i][lengths[i]] = '\0';
        cur += lengths[i];
    }
    
    char out[MAX_BUF] = {0};
    size_t out_len = 0;
    
    uint32_t res = try_cmd(cmd, nstr, out, &out_len);
    
    // Free command strings
    for (uint32_t i = 0; i < nstr; i++) {
        free(cmd[i]);
    }
    free(cmd);
    free(lengths);
    
    assert(out_len <= MAX_BUF);
    uint32_t wlen = (uint32_t)out_len;
    memcpy(&con->writeBuf[0], &wlen, 4);
    memcpy(&con->writeBuf[4], out, out_len);
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

// Fill buffer with data
bool fill_buff(Connection* con) {
    assert(con->read_size < sizeof(con->readBuf));
    
    ssize_t rv = 0;
    do {
        size_t cap = sizeof(con->readBuf) - con->read_size;
        rv = read(con->fd, &con->readBuf[con->read_size], cap);
    } while (rv < 0 && errno == EINTR);
    
    if (rv < 0 && errno == EAGAIN) {
        return false;
    }
    
    if (rv < 0) {
        printf("read() error\n");
        con->state = END;
        return false;
    }
    
    if (rv == 0) {
        if (con->read_size > 0) {
            printf("UNEXPECTED EOF\n");
        } else {
            printf("EOF\n");
        }
        con->state = END;
        return false;
    }
    
    con->read_size += rv;
    assert(con->read_size <= sizeof(con->readBuf));
    
    while (try_req(con)) {
        // Process all complete requests
    }
    
    return (con->state == REQ);
}

// Handle request
void HandleReq(Connection* con) {
    while (fill_buff(con)) {
        // Continue until we can't read more
    }
}

// Handle connection
void HandleConnection(Connection* con) {
    // Update the timer in the connection
    con->idle_start = get_monotonic_usec();
    dlist_detach(&con->idle_list);
    dlist_insert_before(&g_data.idle_list, &con->idle_list);
    
    if (con->state == REQ) {
        printf("Handling request\n");
        HandleReq(con);
    } else if (con->state == RES) {
        printf("Handling response\n");
        HandleRes(con);
    } else {
        assert(0);
    }
} 