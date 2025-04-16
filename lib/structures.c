#include "structures.h"
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

// Get monotonic time in microseconds
uint64_t get_monotonic_usec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000 + (uint64_t)ts.tv_nsec / 1000;
}

// Get next timer in milliseconds
uint64_t next_timer_ms(void) {
    uint64_t now_us = get_monotonic_usec();
    
    // Check idle connections
    if (!dlist_empty(&g_data.idle_list)) {
        Connection* next = container_of(g_data.idle_list.next, Connection, idle_list);
        uint64_t next_us = next->idle_start + 300000; // 5 minutes
        if (next_us <= now_us) {
            return 0;
        }
        return (next_us - now_us) / 1000;
    }
    
    // Check TTL timers
    if (g_data.heap_size > 0) {
        uint64_t next_us = g_data.heap[0].val;
        if (next_us <= now_us) {
            return 0;
        }
        return (next_us - now_us) / 1000;
    }
    
    return 10000; // 10 seconds
}

// Connection done
void conn_done(Connection* conn) {
    // Close the socket
    close(conn->fd);
    
    // Remove from connections array
    if (conn->fd >= 0 && (size_t)conn->fd < sizeof(g_data.connections) / sizeof(g_data.connections[0])) {
        g_data.connections[conn->fd] = NULL;
    }
    
    // Remove from idle list
    dlist_detach(&conn->idle_list);
    
    // Free the connection
    free(conn);
} 