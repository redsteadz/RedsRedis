#include "avl.h"
#include <stdlib.h>

// Helper function to get balance factor
static int get_balance(AVLNode* node) {
    if (node == NULL) return 0;
    return node->balance;
}

// Helper function to initialize a node
void avl_init(AVLNode* node) {
    node->left = NULL;
    node->right = NULL;
    node->parent = NULL;
    node->balance = 0;
}

// Helper function to perform right rotation
static AVLNode* right_rotate(AVLNode* y) {
    AVLNode* x = y->left;
    AVLNode* T2 = x->right;

    x->right = y;
    y->left = T2;

    if (T2) T2->parent = y;
    x->parent = y->parent;
    y->parent = x;

    // Update balance factors
    y->balance = get_balance(y->left) - get_balance(y->right);
    x->balance = get_balance(x->left) - get_balance(x->right);

    return x;
}

// Helper function to perform left rotation
static AVLNode* left_rotate(AVLNode* x) {
    AVLNode* y = x->right;
    AVLNode* T2 = y->left;

    y->left = x;
    x->right = T2;

    if (T2) T2->parent = x;
    y->parent = x->parent;
    x->parent = y;

    // Update balance factors
    x->balance = get_balance(x->left) - get_balance(x->right);
    y->balance = get_balance(y->left) - get_balance(y->right);

    return y;
}

// Function to fix AVL tree balance after insertion/deletion
AVLNode* avl_fix(AVLNode* node) {
    if (node == NULL) return NULL;

    // Update balance factor
    node->balance = get_balance(node->left) - get_balance(node->right);

    // Left Left Case
    if (node->balance > 1 && get_balance(node->left) >= 0)
        return right_rotate(node);

    // Right Right Case
    if (node->balance < -1 && get_balance(node->right) <= 0)
        return left_rotate(node);

    // Left Right Case
    if (node->balance > 1 && get_balance(node->left) < 0) {
        node->left = left_rotate(node->left);
        return right_rotate(node);
    }

    // Right Left Case
    if (node->balance < -1 && get_balance(node->right) > 0) {
        node->right = right_rotate(node->right);
        return left_rotate(node);
    }

    return node;
}

// Function to delete a node from AVL tree
AVLNode* avl_del(AVLNode* node) {
    if (node == NULL) return NULL;

    // Store parent for rebalancing
    AVLNode* parent = node->parent;

    // Node with only one child or no child
    if (node->left == NULL) {
        AVLNode* temp = node->right;
        if (temp) temp->parent = parent;
        free(node);
        return temp;
    } else if (node->right == NULL) {
        AVLNode* temp = node->left;
        if (temp) temp->parent = parent;
        free(node);
        return temp;
    }

    // Node with two children
    AVLNode* temp = node->right;
    while (temp->left != NULL)
        temp = temp->left;

    // Copy the inorder successor's data
    node->balance = temp->balance;

    // Delete the inorder successor
    node->right = avl_del(temp);

    return avl_fix(node);
}

// Function to find node at given offset
AVLNode* avl_offset(AVLNode* node, int64_t offset) {
    if (node == NULL) return NULL;

    int64_t left_size = node->left ? node->left->balance + 1 : 0;

    if (offset == left_size)
        return node;
    else if (offset < left_size)
        return avl_offset(node->left, offset);
    else
        return avl_offset(node->right, offset - left_size - 1);
} 