#include <cstddef>
#pragma once

struct Dlist {
  Dlist *next = NULL;
  Dlist *prev = NULL;
};

inline void dlist_init(Dlist *d) {
  d->next = d;
  d->prev = d;
}

inline bool dlist_empty(Dlist *d) {
  return d->next == d;
}

inline void dlist_detach(Dlist *t){
  Dlist *p = t->prev;
  Dlist *n = t->next;
  p->next = n;
  n->prev = p;
}

inline void dlist_insert_before(Dlist *t, Dlist *n) {
  Dlist *p = t->prev;
  p->next = n;
  n->prev = p;
  n->next = t;
  t->prev = n;
}
