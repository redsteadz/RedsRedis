#pragma once
#include "avl.h"
#include "hash.h"
#include "structures.hpp"
#include <cstddef>

struct ZSet {
  AVLNode *tree = NULL;
  HMap db;
};

// A node with in the ZSet

struct ZNode {
  AVLNode avlnode;
  HNode hnode;
  double score;
  size_t len = 0;
  char name[0];
};

struct HKey {
  HNode node;
  const char *name = NULL;
  size_t len = 0;
};

ZNode *zset_lookup(ZSet *zset, const char *name, size_t len);
