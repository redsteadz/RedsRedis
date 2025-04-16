#ifndef ZSET_H
#define ZSET_H

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>
#include "hash.h"
#include "avl.h"

// ZSet node structure
typedef struct ZNode {
    AVLNode avlnode;
    HNode hnode;
    double score;
    size_t len;
    char name[0];  // Flexible array member
} ZNode;

// ZSet structure
typedef struct ZSet {
    AVLNode *tree;
    HMap db;
} ZSet;

// Function declarations
ZNode *zset_lookup(ZSet *zset, const char *name, size_t len);
bool zset_add(ZSet *zset, const char *name, size_t len, double score);
ZNode *zset_pop(ZSet *zset, char *name, size_t len);
void znode_del(ZNode *znode);
ZNode *zset_query(ZSet *zset, double score, const char *name, size_t len);
ZNode *znode_offset(ZNode *node, int64_t offset);
void zset_dispose(ZSet *zset);

// Helper functions from AVL module
void avl_init(AVLNode *node);
AVLNode *avl_fix(AVLNode *node);
AVLNode *avl_del(AVLNode *node);
AVLNode *avl_offset(AVLNode *node, int64_t offset);

#endif /* ZSET_H */

