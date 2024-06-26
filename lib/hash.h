#pragma once

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>
using namespace std;

// hashtable node, should be embedded into the payload
struct HNode {
  HNode *next = NULL;
  uint64_t hcode = 0;
};

// a simple fixed-sized hashtable
struct HTab {
  HNode **tab = NULL;
  size_t mask = 0;
  size_t size = 0;
};

// the real hashtable interface.
// it uses 2 hashtables for progressive resizing.
struct HMap {
  HTab ht1; // newer
  HTab ht2; // older
  size_t resizing_pos = 0;
};


HNode *hm_find(HMap *hmap, HNode *key, bool (*eq)(HNode *, HNode *));
void hm_insert(HMap *hmap, HNode *node);
HNode *hm_pop(HMap *hmap, HNode *key, bool (*eq)(HNode *, HNode *));
size_t hm_size(HMap *hmap);
void h_scan(HTab *htab, void (*pack)(HNode *, void *container), void *container);
void hm_destroy(HMap *hmap);

#define container_of(ptr, T, member) \
    (T *)( (char *)ptr - offsetof(T, member) )
