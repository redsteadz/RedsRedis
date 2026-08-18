# RedsRedis

RedsRedis is a Linux-focused, Redis-inspired key-value server written in C++14. It is an educational implementation of non-blocking sockets, polling, binary request/response framing, timers, and custom data structures—not a drop-in Redis server or Redis protocol implementation.

## Implemented systems

- A non-blocking TCP server using `poll`
- A custom hash table for string keys
- Sorted sets backed by a hash table and AVL tree
- Millisecond key expiry tracked with a heap
- A small typed response format for strings, integers, arrays, errors, and nil values
- An interactive command-line client

The server listens on `127.0.0.1:1800`.

## Commands

Commands are lowercase in the current parser.

| Command | Purpose |
| --- | --- |
| `set key value` | Create or replace a string value |
| `get key` | Read a string value |
| `del key` | Delete a key |
| `keys` | List stored values from the hash table |
| `pexpire key milliseconds` | Set a millisecond expiry |
| `pttl key` | Read the remaining expiry in milliseconds |
| `zadd set score member` | Add or update a sorted-set member |
| `zscore set member` | Read a member's score |
| `zquery set score member offset limit` | Query sorted-set entries from a score/member position |

## Build and run

RedsRedis uses POSIX socket APIs and is intended for Linux or a compatible Unix-like environment.

~~~bash
git clone https://github.com/redsteadz/RedsRedis.git
cd RedsRedis

cmake -S . -B build
cmake --build build
g++ -std=c++14 client.cpp -o build/Client
~~~

Start the server and client in separate terminals:

~~~bash
./build/Server
./build/Client
~~~

Then enter commands such as:

~~~text
set language cpp
get language
pexpire language 5000
pttl language
~~~

## Scope

The project currently stores data in memory and uses its own wire format. Persistence, replication, clustering, and Redis-client compatibility are outside the current implementation.
