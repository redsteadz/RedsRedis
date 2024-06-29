cmake_minimum_required(VERSION 3.10)
project(Server)

set(CMAKE_CXX_STANDARD 14)

add_executable(Server server.cpp lib/hash.cpp lib/Zset.cpp lib/avl.cpp lib/heap.cpp lib/thread.cpp)

target_link_libraries(Server pthread)
