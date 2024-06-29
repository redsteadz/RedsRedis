#include "lib/dlist.h"
#include "lib/functions.hpp"
#include "lib/structures.hpp"
#include "poll.h"
#include <arpa/inet.h>
#include <cassert>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <netinet/ip.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>
using namespace std;


int main(int argc, char *argv[]) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  dlist_init(&g_data.idle_list);
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
  if (bind(fd, (const sockaddr *)&addr, sizeof(addr)) < 0) {
    perror("bind");
    return 1;
  }

  if (listen(fd, 10) < 0) {
    perror("listen");
    return 1;
  }

  fd_set_nb(fd);

  vector<struct pollfd> fds;

  while (true) {
    // Poll all the value connections
    fds.clear();
    struct pollfd listener = {fd, POLLIN, 0};
    fds.push_back(listener);
    for (Connection *c : g_data.connections) {
      if (!c)
        continue;
      struct pollfd conn = {};
      // Make it a non blocking connection
      conn.fd = c->fd;
      conn.events = (c->state == REQ) ? POLLIN : POLLOUT;
      conn.events = conn.events | POLLERR;
      fds.push_back(conn);
    }
    int timeout = (int)next_timer_ms();
    int rv = poll(fds.data(), (nfds_t)fds.size(), timeout);

    if (rv < 0){
      perror("poll");
    }

    for (int  i = 1; i < fds.size(); i++){
      if (fds[i].revents){
        Connection *con = g_data.connections[fds[i].fd];
        cout << "Handling " << fds[i].fd << endl;
        //DONE: Implement Connection Handling 
        //Update the timer in the connection
        HandleConnection(con);
        if (con->state == END){
          // If the connection is about to end 
          // g_data.connections[con->fd] = nullptr;
          // close(con->fd);
          // free(con);
          conn_done(con);
        }
      }
    }
    process_timers();
    
    if (fds[0].revents){
      //DONE: Accept new connection (Accept, Make the struct, push to vector );
      acceptConnection(fd , g_data.connections);
    }
    // cout << "Accepted all g_data.connections" << endl;

  }
  
  return 0;
}
