#include "avl.h"
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>

static uint32_t avl_depth(AVLNode *node) {
  if (node)
    return node->depth;
  return 0;
}

static uint32_t max(uint32_t a, uint32_t b) { return a > b ? a : b; }

static uint32_t avl_cnt(AVLNode *node) {
  if (node)
    return node->cnt;
  return 0;
}

static void avl_update(AVLNode *node) {
  node->depth = max(avl_depth(node->left), avl_depth(node->right)) + 1;
  node->cnt = avl_cnt(node->left) + avl_cnt(node->right) + 1;
}

static AVLNode *left_rotate(AVLNode *node) {
  AVLNode *right = node->right;
  AVLNode *right_left = right->left;
  if (right_left)
    right_left->parent = node;
  right->left = node;
  node->right = right_left;
  right->parent = node->parent;
  node->parent = right;
  avl_update(node);
  avl_update(right);

  return right;
}

static AVLNode *right_rotate(AVLNode *node) {
  AVLNode *left = node->left;
  AVLNode *left_right = left->right;
  if (left_right)
    left_right->parent = node;
  left->right = node;
  node->left = left_right;
  left->parent = node->parent;
  node->parent = left;
  avl_update(node);
  avl_update(left);

  return left;
}

static AVLNode *avl_fix_left(AVLNode *root) {
  if (avl_depth(root->left->left) < avl_depth(root->left->right)) {
    root->left = left_rotate(root->left);
  }

  return right_rotate(root);
}

static AVLNode *avl_fix_right(AVLNode *root) {
  if (avl_depth(root->right->right) < avl_depth(root->right->left)) {
    root->right = right_rotate(root->right);
  }
  return left_rotate(root);
}

AVLNode *avl_fix(AVLNode *node) {
  while (true) {
    avl_update(node);
    uint32_t l = avl_depth(node->left);
    uint32_t r = avl_depth(node->right);
    AVLNode **from = NULL;

    if (AVLNode *p = node->parent) {
      // Save parents pointer
      from = (p->left == node) ? &p->left : &p->right;
    }
    if (l == r + 2) {
      node = avl_fix_left(node);
    } else if (l + 2 == r)
      node = avl_fix_right(node);

    // IF root node
    if (!from)
      break;
    *from = node;

    // Move on to it's parent
    node = node->parent;
  }

  return node;
}

AVLNode *avl_del(AVLNode *node) {
  if (node->right == NULL) {
    // no right subtree, replace the node with the left subtree
    // link the left subtree to the parent
    AVLNode *parent = node->parent;
    if (node->left) {
      node->left->parent = parent;
    }
    if (parent) { // attach the left subtree to the parent
      (parent->left == node ? parent->left : parent->right) = node->left;
      return avl_fix(parent); // AVL-specific!
    } else {                  // removing root?
      return node->left;
    }
  } else {
    // detach the successor
    AVLNode *victim = node->right;
    while (victim->left) {
      victim = victim->left;
    }
    AVLNode *root = avl_del(victim);
    // swap with it
    *victim = *node;
    if (victim->left) {
      victim->left->parent = victim;
    }
    if (victim->right) {
      victim->right->parent = victim;
    }
    if (AVLNode *parent = node->parent) {
      (parent->left == node ? parent->left : parent->right) = victim;
      return root;
    } else { // removing root?
      return victim;
    }
  }
}

AVLNode *avl_offset(AVLNode *node, int64_t offset) {
  int64_t pos = 0; // relative to the starting node
  while (offset != pos) {
    if (pos < offset && pos + avl_cnt(node->right) >= offset) {
      // the target is inside the right subtree
      node = node->right;
      pos += avl_cnt(node->left) + 1;
    } else if (pos > offset && pos - avl_cnt(node->left) <= offset) {
      // the target is inside the left subtree
      node = node->left;
      pos -= avl_cnt(node->right) + 1;
    } else {
      // go to the parent
      AVLNode *parent = node->parent;
      if (!parent) {
        return NULL; // out of range
      }
      if (parent->right == node) {
        pos -= avl_cnt(node->left) + 1;
      } else {
        pos += avl_cnt(node->right) + 1;
      }
      node = parent;
    }
  }
  return node;
}
