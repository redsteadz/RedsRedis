#ifndef DLIST_H
#define DLIST_H

#include <stdbool.h>

// Double linked list node
typedef struct Dlist {
    struct Dlist* next;
    struct Dlist* prev;
} Dlist;

// Initialize a double linked list
static inline void dlist_init(Dlist* d) {
    d->next = d;
    d->prev = d;
}

// Check if list is empty
static inline bool dlist_empty(Dlist* d) {
    return d->next == d;
}

// Detach a node from list
static inline void dlist_detach(Dlist* t) {
    Dlist* p = t->prev;
    Dlist* n = t->next;
    p->next = n;
    n->prev = p;
}

// Insert a node before target
static inline void dlist_insert_before(Dlist* t, Dlist* n) {
    Dlist* p = t->prev;
    p->next = n;
    n->prev = p;
    n->next = t;
    t->prev = n;
}

#endif // DLIST_H
