#ifndef HEAP_H
#define HEAP_H

#include <stddef.h>
#include <stdint.h>

// Heap item structure
typedef struct HeapItem {
    uint64_t val;
    size_t* ref;
} HeapItem;

// Update heap at position
void heap_update(HeapItem* a, size_t pos, size_t len);

#endif // HEAP_H
