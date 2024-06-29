#include <cstddef>
#include <vector>
#include <deque>
#include <pthread.h>
struct Work {
  void (*f) (void *) = NULL;
  void *arg = NULL;
};

struct ThreadPool {
  std::vector<pthread_t> threads;
  std::deque<Work> queue;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
};

void thread_pool_init(ThreadPool *tp, size_t num_threads);
void thread_pool_queue(ThreadPool *tp, void (*f)(void *), void *arg);
