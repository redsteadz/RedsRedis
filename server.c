#include "lib/dlist.h"
#include "lib/functions.h"
#include "lib/structures.h"
#include "lib/hash.h"
#include "poll.h"
#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/ip.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <unistd.h>

// Global data instance
GlobalData g_data;

int main(int argc, char *argv[]) {
  // Initialize global data structures
  memset(&g_data, 0, sizeof(GlobalData));
  hm_init(&g_data.db);
  dlist_init(&g_data.idle_list);
  thread_pool_init(&g_data.tp, 4);  // Initialize thread pool with 4 threads

  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    perror("socket");
    return 1;
  }
  int val = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
  struct sockaddr_in addr;
  addr.sin_family = AF_INET;
  addr.sin_port = htons(1800);
  addr.sin_addr.s_addr = inet_addr("127.0.0.1");
  if (bind(fd, (const struct sockaddr *)&addr, sizeof(addr)) < 0) {
    perror("bind");
    return 1;
  }

  if (listen(fd, 10) < 0) {
    perror("listen");
    return 1;
  }

  fd_set_nb(fd);

  struct pollfd *fds = NULL;
  size_t fds_capacity = 0;
  size_t fds_size = 0;
  g_data.connections_size = 100;

  while (1) {
    // Poll all the value connections
    if (fds_capacity < g_data.connections_size + 1) {
      size_t new_capacity = (fds_capacity == 0) ? 16 : fds_capacity * 2;
      struct pollfd *new_fds = realloc(fds, new_capacity * sizeof(struct pollfd));
      if (!new_fds) {
        perror("realloc");
        exit(1);
      }
      fds = new_fds;
      fds_capacity = new_capacity;
    }
    
    fds_size = 0;
    printf("fds_size: %zu\n", fds_size);
    // Add listener to poll array
    fds[fds_size].fd = fd;
    fds[fds_size].events = POLLIN;
    fds[fds_size].revents = 0;
    fds_size++;

    printf("g_data.connections_size: %zu\n", g_data.connections_size);

    // Add connections to poll array
    for (size_t i = 0; i < g_data.connections_size; i++) {
      Connection *c = g_data.connections[i];
      // printf("c: %p\n", c);
      if (!c) continue;

      if (c->state == RES) {
        printf("Adding connection %d to poll\n", c->fd);
      }
      
      printf("Adding connection %d to poll\n", c->fd);
      fds[fds_size].fd = c->fd;
      fds[fds_size].events = (c->state == REQ) ? POLLIN : POLLOUT;
      fds[fds_size].events |= POLLERR;
      fds[fds_size].revents = 0;
      fds_size++;
    }
    
    int timeout = (int)next_timer_ms();
    // printf("Timeout: %d\n", timeout);
    int rv = poll(fds, (nfds_t)fds_size, timeout);

    if (rv < 0) {
      perror("poll");
    }

    for (size_t i = 1; i < fds_size; i++) {
      if (fds[i].revents) {
        printf("revents: %d\n", fds[i].revents);
        Connection *con = g_data.connections[fds[i].fd];
        printf("Handling %d\n", fds[i].fd);
        
        // Update the timer in the connection
        HandleConnection(con);
        if (con->state == END) {
          // If the connection is about to end
          conn_done(con);
        }
      }
    }
    
    process_timers();
    
    if (fds[0].revents) {
      // Accept new connection
      printf("Accepting new connection\n");
      acceptConnection(fd, g_data.connections, &g_data.connections_size);
      printf("size: %zu\n", g_data.connections_size);
    }
  }
  
  free(fds);
  return 0;
} 