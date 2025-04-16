#include "hash.h"
#include <stdlib.h>
#include <string.h>

// Initialize a hash map
void hm_init(HMap* hmap) {
    memset(hmap, 0, sizeof(HMap));
}

// FNV-1a hash function
uint64_t str_hash(const uint8_t* data, size_t len) {
    uint64_t h = 0x811C9DC5;
    for (size_t i = 0; i < len; i++) {
        h ^= data[i];
        h *= 0x01000193;
    }
    return h;
}

#define INITIAL_SIZE 16

// Initialize a hashtable
static void h_init(HTab* htab, size_t n) {
    htab->tab = (HNode**)calloc(n, sizeof(HNode*));
    htab->mask = n - 1;
    htab->size = 0;
}

// Insert a node into a hashtable
static void h_insert(HTab* htab, HNode* node) {
    size_t pos = node->hcode & htab->mask;
    HNode* next = htab->tab[pos];
    node->next = next;
    htab->tab[pos] = node;
    htab->size++;
}

// Look up a node in a hashtable
static HNode* h_lookup(HTab* htab, HNode* key, bool (*eq)(HNode*, HNode*)) {
    if (!htab->tab) {
        return NULL;
    }

    size_t pos = key->hcode & htab->mask;
    HNode* cur = htab->tab[pos];
    while (cur) {
        if (cur->hcode == key->hcode && eq(cur, key)) {
            return cur;
        }
        cur = cur->next;
    }
    return NULL;
}

// Move nodes from ht2 to ht1 during resizing
static void hm_help_resizing(HMap* hmap) {
    if (!hmap->ht2.tab) {
        return;
    }

    size_t nwork = 0;
    while (nwork < 4 && hmap->ht2.size) {
        HNode* node = hmap->ht2.tab[hmap->resizing_pos];
        if (!node) {
            hmap->resizing_pos++;
            continue;
        }

        hmap->ht2.tab[hmap->resizing_pos] = node->next;
        hmap->ht2.size--;
        h_insert(&hmap->ht1, node);
        nwork++;
    }

    if (hmap->ht2.size == 0) {
        free(hmap->ht2.tab);
        hmap->ht2.tab = NULL;
    }
}

// Find a node in the hash map
HNode* hm_find(HMap* hmap, HNode* key, bool (*eq)(HNode*, HNode*)) {
    hm_help_resizing(hmap);
    HNode* node = h_lookup(&hmap->ht1, key, eq);
    if (node) {
        return node;
    }
    return h_lookup(&hmap->ht2, key, eq);
}

// Insert a node into the hash map
void hm_insert(HMap* hmap, HNode* node) {
    if (!hmap->ht1.tab) {
        h_init(&hmap->ht1, INITIAL_SIZE);
    }
    h_insert(&hmap->ht1, node);
}

// Remove a node from the hash map
HNode* hm_pop(HMap* hmap, HNode* key, bool (*eq)(HNode*, HNode*)) {
    hm_help_resizing(hmap);
    HNode* node = h_lookup(&hmap->ht1, key, eq);
    if (node) {
        size_t pos = node->hcode & hmap->ht1.mask;
        hmap->ht1.tab[pos] = node->next;
        hmap->ht1.size--;
        return node;
    }
    node = h_lookup(&hmap->ht2, key, eq);
    if (node) {
        size_t pos = node->hcode & hmap->ht2.mask;
        hmap->ht2.tab[pos] = node->next;
        hmap->ht2.size--;
        return node;
    }
    return NULL;
}

// Get the size of the hash map
size_t hm_size(HMap* hmap) {
    return hmap->ht1.size + hmap->ht2.size;
}

// Scan all nodes in a hashtable
void h_scan(HTab* htab, void (*pack)(HNode*, void*), void* container) {
    if (!htab->tab) {
        return;
    }

    for (size_t i = 0; i <= htab->mask; i++) {
        HNode* node = htab->tab[i];
        while (node) {
            pack(node, container);
            node = node->next;
        }
    }
}

// Destroy the hash map
void hm_destroy(HMap* hmap) {
    free(hmap->ht1.tab);
    free(hmap->ht2.tab);
    memset(hmap, 0, sizeof(HMap));
} 