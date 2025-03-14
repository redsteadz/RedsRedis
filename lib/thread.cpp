#include "thread.h"
#include <cassert>
#include <cstddef>
#include <deque>
#include <pthread.h>
#include <vector>

static void *worker(void *arg) {
  ThreadPool *tp = (ThreadPool *)arg;
  while (true) {
    pthread_mutex_lock(&tp->mutex);
    while (tp->queue.empty()) {
      pthread_cond_wait(&tp->cond, &tp->mutex);
    }
    Work w = tp->queue.front();
    tp->queue.pop_front();
    pthread_mutex_unlock(&tp->mutex);

    w.f(w.arg);
  }

  return NULL;
}

void thread_pool_init(ThreadPool *tp, size_t num_threads) {
  assert(num_threads > 0);
  int rv = pthread_mutex_init(&tp->mutex, NULL);
  assert(rv == 0);
  rv = pthread_cond_init(&tp->cond, NULL);
  assert(rv == 0);
  tp->threads.resize(num_threads);
  for (size_t i = 0; i < num_threads; i++) {
    int rv = pthread_create(&tp->threads[i], NULL, &worker, tp);
  }
}

void thread_pool_queue(ThreadPool *tp, void (*f)(void *), void *arg) {
    Work w;
    w.f = f;
    w.arg = arg;

    pthread_mutex_lock(&tp->mutex);
    tp->queue.push_back(w);
    pthread_cond_signal(&tp->cond);
    pthread_mutex_unlock(&tp->mutex);
}
