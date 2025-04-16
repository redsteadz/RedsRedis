#ifndef THREAD_H
#define THREAD_H

#include <pthread.h>
#include <stddef.h>

#define MAX_THREADS 32
#define MAX_QUEUE_SIZE 1024

typedef struct {
    void (*f)(void*);
    void* arg;
} Work;

typedef struct {
    pthread_t threads[MAX_THREADS];
    Work queue[MAX_QUEUE_SIZE];
    size_t queue_size;
    size_t queue_front;
    size_t queue_rear;
    size_t num_threads;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int shutdown;
} ThreadPool;

// Initialize the thread pool with specified number of threads
void thread_pool_init(ThreadPool* tp, size_t num_threads);

// Add a work item to the thread pool queue
int thread_pool_queue(ThreadPool* tp, void (*f)(void*), void* arg);

// Clean up and destroy the thread pool
void thread_pool_destroy(ThreadPool* tp);

#endif // THREAD_H
