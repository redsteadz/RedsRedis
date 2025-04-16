#ifndef AVL_H
#define AVL_H

#include <stddef.h>
#include <stdint.h>

// AVL tree node structure
typedef struct AVLNode {
    struct AVLNode* left;
    struct AVLNode* right;
    struct AVLNode* parent;
    int32_t balance;
} AVLNode;

// Function declarations
void avl_init(AVLNode* node);
AVLNode* avl_fix(AVLNode* node);
AVLNode* avl_del(AVLNode* node);
AVLNode* avl_offset(AVLNode* node, int64_t offset);

#endif // AVL_H
