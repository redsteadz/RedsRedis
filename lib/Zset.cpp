#include "Zset.h"
#include "avl.h"

static ZNode *znode_new(const char *name, size_t len, double score) {
  ZNode *znode = (ZNode *)malloc(sizeof(ZNode) + len);
  znode->score = score;
  znode->len = len;
  memcpy(znode->name, name, len);

  // Nodes
  znode->hnode.next = NULL;
  znode->hnode.hcode = str_hash((uint8_t *)name, len);
  avl_init(&znode->avlnode);
  return znode;
}

static bool hcmp(HNode *node, HNode *key) {
  ZNode *znode = container_of(node, ZNode, hnode);
  HKey *hkey = container_of(key, HKey, node);
  if (znode->len != hkey->len) {
    return false;
  }
  return 0 == memcmp(znode->name, hkey->name, znode->len);
}

// Finds the element in the ZSet
ZNode *zset_lookup(ZSet *zset, const char *name, size_t len) {
  if (!zset->tree) {
    return NULL;
  }
  // A Helper struct to hold some basic information
  HKey key;
  // Find using the hcode and the name and len
  key.node.hcode = str_hash((uint8_t *)name, len);
  key.name = name;
  key.len = len;
  // Within this ZSets HMap DB. Is there a node such that the container of them
  // have the same name
  HNode *found = hm_find(&zset->db, &key.node, &hcmp);
  return found ? container_of(found, ZNode, hnode) : NULL;
}

static bool zless(AVLNode *lhs, double score, const char *name, size_t len) {
  ZNode *zl = container_of(lhs, ZNode, avlnode);
  if (zl->score != score) {
    return zl->score < score;
  }
  int rv = memcmp(zl->name, name, min(zl->len, len));
  if (rv != 0) {
    return rv < 0;
  }
  return zl->len < len;
}

static bool zless(AVLNode *lhs, AVLNode *rhs) {
  ZNode *zr = container_of(rhs, ZNode, avlnode);
  return zless(lhs, zr->score, zr->name, zr->len);
}

static void tree_add(ZSet *zset, ZNode *znode) {
  AVLNode *cur = NULL;
  AVLNode **from = &zset->tree;
  while (*from) {
    cur = *from;
    from = zless(&znode->avlnode, cur) ? &cur->left : &cur->right;
  }
  *from = &znode->avlnode;
  znode->avlnode.parent = cur;
  zset->tree = avl_fix(&znode->avlnode);
}

static void zset_update(ZSet *zset, ZNode *node, double score) {
  if (node->score == score) {
    return;
  }
  zset->tree = avl_del(&node->avlnode);
  node->score = score;
  avl_init(&node->avlnode);
  tree_add(zset, node);
}

bool zset_add(ZSet *zset, const char *name, size_t len, double score) {
  ZNode *node = zset_lookup(zset, name, len);
  if (node) { // update the score of an existing pair
    zset_update(zset, node, score);
    return false;
  } else { // add a new node
    node = znode_new(name, len, score);
    hm_insert(&zset->db, &node->hnode);
    tree_add(zset, node);
    return true;
  }
}
