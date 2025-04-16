#ifndef HASH_H
#define HASH_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

// hashtable node, should be embedded into the payload
typedef struct HNode {
    struct HNode* next;
    uint64_t hcode;
} HNode;

// Key structure for hash table
typedef struct HKey {
    HNode node;
    const char* name;
    size_t len;
} HKey;

// a simple fixed-sized hashtable
typedef struct HTab {
    HNode** tab;
    size_t mask;
    size_t size;
} HTab;

// the real hashtable interface.
// it uses 2 hashtables for progressive resizing.
typedef struct HMap {
    HTab ht1;  // newer
    HTab ht2;  // older
    size_t resizing_pos;
} HMap;

// Function declarations
void hm_init(HMap* hmap);  // Initialize a hash map
uint64_t str_hash(const uint8_t* data, size_t len);
HNode* hm_find(HMap* hmap, HNode* key, bool (*eq)(HNode*, HNode*));
void hm_insert(HMap* hmap, HNode* node);
HNode* hm_pop(HMap* hmap, HNode* key, bool (*eq)(HNode*, HNode*));
size_t hm_size(HMap* hmap);
void h_scan(HTab* htab, void (*pack)(HNode*, void* container), void* container);
void hm_destroy(HMap* hmap);

// Helper macro for container_of
#define container_of(ptr, T, member) \
    ((T*)((char*)(ptr) - offsetof(T, member)))

#endif // HASH_H
