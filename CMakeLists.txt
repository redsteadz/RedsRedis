cmake_minimum_required(VERSION 3.10)
project(Server C)

set(CMAKE_C_STANDARD 11)

# Create a list of source files
set(SOURCES
    server.c
    lib/hash.c
    lib/Zset.c
    lib/avl.c
    lib/structures.c
    lib/functions.c
    lib/dlist.c
    lib/heap.c
    lib/thread.c
)

# Create the executable
add_executable(Server ${SOURCES})

# Link with pthread
target_link_libraries(Server pthread)
