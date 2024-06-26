#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>

#define container_of(ptr, T, member) (T *)((char *)ptr - offsetof(T, member))

#include "avl.h"


static uint32_t avl_depth(AVLNode *node) {
  if (node)
    return node->depth;
  return 0;
}

static uint32_t avl_cnt(AVLNode *node) {
  if (node)
    return node->cnt;
  return 0;
}

static void avl_update(AVLNode *node) {
  node->depth = std::max(node->left->depth, node->right->depth) + 1;
  node->cnt = node->left->cnt + node->right->cnt + 1;
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

struct Data {
  AVLNode node;
  uint32_t val = 0;
};

struct AVLTree {
  AVLNode *root = NULL;
};

static void add(AVLTree &c, uint32_t val) {
  Data *data = new Data(); // allocate the data
  avl_init(&data->node);
  data->val = val;

  AVLNode *cur = NULL;      // current node
  AVLNode **from = &c.root; // the incoming pointer to the next node
  while (*from) {           // tree search
    cur = *from;
    uint32_t node_val = (container_of(cur, Data, node))->val;
    from = (val < node_val) ? &cur->left : &cur->right;
  }
  *from = &data->node; // attach the new node
  data->node.parent = cur;
  c.root = avl_fix(&data->node);
}


static bool del(AVLTree &c, uint32_t val) {
    AVLNode *cur = c.root;
    while (cur) {
        uint32_t node_val = (container_of(cur, Data, node))->val;
        if (val == node_val) {
            break;
        }
        cur = val < node_val ? cur->left : cur->right;
    }
    if (!cur) {
        return false;
    }

    c.root = avl_del(cur);
    delete container_of(cur, Data, node);
    return true;
}

static void avl_verify(AVLNode *parent, AVLNode *node) {
    if (!node) {
        return;
    }
    // verify subtrees recursively
    avl_verify(node, node->left);
    avl_verify(node, node->right);
    // 1. The parent pointer is correct.
    assert(node->parent == parent);
    // 2. The auxiliary data is correct.
    assert(node->cnt == 1 + avl_cnt(node->left) + avl_cnt(node->right));
    uint32_t l = avl_depth(node->left);
    uint32_t r = avl_depth(node->right);
    assert(node->depth == 1 + std::max(l, r));
    // 3. The height invariant is OK.
    assert(l == r || l + 1 == r || l == r + 1);
    // 4. The data is ordered.
    uint32_t val = (container_of(node, Data, node))->val;
    if (node->left) {
        assert(node->left->parent == node);
        assert((container_of(node->left, Data, node))->val <= val);
    }
    if (node->right) {
        assert(node->right->parent == node);
        assert((container_of(node->right, Data, node))->val >= val);
    }
}
