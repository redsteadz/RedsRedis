#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdint.h>

#define MAX_BUF 4096
#define MAX_ARGS 32

// Response types
typedef enum {
    SER_NIL = 0,
    SER_ERR = 1,
    SER_STR = 2,
    SER_INT = 3,
    SER_ARR = 4
} Type;

// Response structure
typedef struct {
    uint32_t len;
    char* msg;
} Packet;

// Function to write all data to socket
static int32_t writeAll(int fd, const char* buf, size_t n) {
    while (n > 0) {
        ssize_t writeSize = write(fd, buf, n);
        if (writeSize <= 0) {
            return -1;
        }
        n -= (size_t)writeSize;
        buf += writeSize;
    }
    return 0;
}

// Function to read all data from socket
static int32_t readAll(int fd, char* buf, size_t n) {
    while (n > 0) {
        ssize_t readSize = read(fd, buf, n);
        if (readSize <= 0) {
            return -1;
        }
        n -= (size_t)readSize;
        buf += readSize;
    }
    return 0;
}

// Parse received data into a packet
static Packet parseData(const char* buff) {
    Packet packet = {0};
    memcpy(&packet.len, buff, 4);
    packet.msg = malloc(packet.len + 1);
    if (!packet.msg) {
        packet.len = 0;
        return packet;
    }
    memcpy(packet.msg, buff + 4, packet.len);
    packet.msg[packet.len] = '\0';
    return packet;
}

// Read a packet from socket
static int32_t readIO(int fd, Packet* packet) {
    char buff[MAX_BUF] = {0};
    int32_t n = MAX_BUF;
    
    ssize_t rs = read(fd, buff, n);
    if (rs <= 0) {
        return -1;
    }
    
    *packet = parseData(buff);
    return 0;
}

// Send a query to the server
static int32_t query(int fd, const char* text) {
    uint32_t messageLen = (uint32_t)strlen(text);
    char writeBuff[4 + messageLen];
    memcpy(writeBuff, &messageLen, 4);
    memcpy(writeBuff + 4, text, messageLen);
    
    if (writeAll(fd, writeBuff, 4 + messageLen) < 0) {
        return -1;
    }
    
    Packet readPacket = {0};
    int32_t err = readIO(fd, &readPacket);
    if (err) {
        if (errno == 0) {
            printf("EOF\n");
        } else {
            printf("read() error: %s\n", strerror(errno));
        }
        free(readPacket.msg);
        return err;
    }
    
    printf("Server Says: %s\n", readPacket.msg);
    free(readPacket.msg);
    return 0;
}

// Process server response
static uint32_t out_resp(const char* rbuf, int32_t size) {
    Type resType = (Type)rbuf[0];
    uint32_t len;
    
    switch (resType) {
        case SER_STR:
        case SER_ERR:
            if (size < 1 + 4) {
                printf("Bad Response\n");
                return -1;
            }
            memcpy(&len, &rbuf[1], 4);
            printf("[str] - len: %d, res: %.*s\n", size, len, &rbuf[5]);
            return 4 + len + 1;
            
        case SER_NIL:
            printf("[nil] - len: %d, res: nil\n", size);
            return 1;
            
        case SER_INT:
            if (size < 1 + 8) {
                printf("Bad Response\n");
                return -1;
            }
            uint64_t val;
            memcpy(&val, &rbuf[1], 8);
            printf("[int] - len: %d, res: %lu\n", size, val);
            return 8 + 1;
            
        case SER_ARR:
            if (size < 1 + 4) {
                printf("Bad Response\n");
                return -1;
            }
            memcpy(&len, &rbuf[1], 4);
            uint32_t loc = 4 + 1;
            for (uint32_t i = 0; i < len; i++) {
                uint32_t rv = out_resp(&rbuf[loc], size - loc);
                if (rv < 0) {
                    return rv;
                }
                loc += rv;
            }
            return loc;
            
        default:
            printf("Unknown response type: %d\n", resType);
            return -1;
    }
}

// Read and process server response
static int32_t read_res(int fd) {
    char rbuf[4 + MAX_BUF];
    
    if (readAll(fd, rbuf, 4) < 0) {
        if (errno == 0) {
            printf("EOF\n");
        } else {
            printf("read() error: %s\n", strerror(errno));
        }
        return -1;
    }

    uint32_t size = 0;
    memcpy(&size, rbuf, 4);

    if (readAll(fd, &rbuf[4], size) < 0) {
        if (errno == 0) {
            printf("EOF\n");
        } else {
            printf("read() error: %s\n", strerror(errno));
        }
        return -1;
    }
    
    return out_resp(&rbuf[4], size);
}

// Send a request to the server
static int32_t sendReq(int fd, int32_t nstr, char** cmd) {
    uint32_t packetSize = 4;
    for (int32_t i = 0; i < nstr; i++) {
        packetSize += strlen(cmd[i]) + 4;
    }
    
    char* packet = malloc(packetSize);
    if (!packet) {
        return -1;
    }
    
    memcpy(packet, &nstr, 4);
    int cur = 4;
    
    for (int32_t i = 0; i < nstr; i++) {
        int l = strlen(cmd[i]);
        memcpy(&packet[cur], &l, 4);
        cur += 4;
        memcpy(&packet[cur], cmd[i], l);
        cur += l;
    }
    
    int32_t result = writeAll(fd, packet, packetSize);
    free(packet);
    return result;
}

// Interactive query function
static int32_t interactive_query(int fd) {
    char line[MAX_BUF];
    char* args[MAX_ARGS];
    int arg_count = 0;
    
    if (!fgets(line, sizeof(line), stdin)) {
        return 0;
    }
    
    // Remove newline
    line[strcspn(line, "\n")] = 0;
    
    // Split into arguments
    char* token = strtok(line, " ");
    while (token && arg_count < MAX_ARGS) {
        args[arg_count++] = token;
        token = strtok(NULL, " ");
    }
    
    if (arg_count > 0) {
        int32_t err = sendReq(fd, arg_count, args);
        if (err) {
            printf("Request error\n");
            return err;
        }
        
        err = read_res(fd);
        if (err < 0) {
            printf("Receive error\n");
            return err;
        }
    }
    
    return 1;
}

int main(int argc, char* argv[]) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(1800);
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");

    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(fd);
        return 1;
    }

    printf("Connected to server. Type commands (Ctrl+C to exit):\n");
    
    while (interactive_query(fd)) {
    }

    shutdown(fd, SHUT_RDWR);
    close(fd);
    return 0;
}
