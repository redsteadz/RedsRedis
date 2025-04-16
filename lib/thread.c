#include "thread.h"
#include <stdlib.h>
#include <string.h>

// Worker thread function
static void* worker_thread(void* arg) {
    ThreadPool* tp = (ThreadPool*)arg;
    Work work;

    while (1) {
        pthread_mutex_lock(&tp->mutex);
        
        // Wait for work or shutdown
        while (tp->queue_size == 0 && !tp->shutdown) {
            pthread_cond_wait(&tp->cond, &tp->mutex);
        }

        // Check for shutdown
        if (tp->shutdown) {
            pthread_mutex_unlock(&tp->mutex);
            pthread_exit(NULL);
        }

        // Get work from queue
        work = tp->queue[tp->queue_front];
        tp->queue_front = (tp->queue_front + 1) % MAX_QUEUE_SIZE;
        tp->queue_size--;

        pthread_mutex_unlock(&tp->mutex);

        // Execute the work
        if (work.f != NULL) {
            work.f(work.arg);
        }
    }

    return NULL;
}

void thread_pool_init(ThreadPool* tp, size_t num_threads) {
    if (num_threads > MAX_THREADS) {
        num_threads = MAX_THREADS;
    }

    // Initialize thread pool structure
    memset(tp, 0, sizeof(ThreadPool));
    tp->num_threads = num_threads;
    tp->queue_size = 0;
    tp->queue_front = 0;
    tp->queue_rear = 0;
    tp->shutdown = 0;

    // Initialize synchronization objects
    pthread_mutex_init(&tp->mutex, NULL);
    pthread_cond_init(&tp->cond, NULL);

    // Create worker threads
    for (size_t i = 0; i < num_threads; i++) {
        if (pthread_create(&tp->threads[i], NULL, worker_thread, tp) != 0) {
            thread_pool_destroy(tp);
            return;
        }
    }
}

int thread_pool_queue(ThreadPool* tp, void (*f)(void*), void* arg) {
    pthread_mutex_lock(&tp->mutex);

    // Check if queue is full
    if (tp->queue_size >= MAX_QUEUE_SIZE) {
        pthread_mutex_unlock(&tp->mutex);
        return -1;
    }

    // Add work to queue
    Work work = {f, arg};
    tp->queue[tp->queue_rear] = work;
    tp->queue_rear = (tp->queue_rear + 1) % MAX_QUEUE_SIZE;
    tp->queue_size++;

    // Signal waiting thread
    pthread_cond_signal(&tp->cond);
    pthread_mutex_unlock(&tp->mutex);

    return 0;
}

void thread_pool_destroy(ThreadPool* tp) {
    pthread_mutex_lock(&tp->mutex);
    tp->shutdown = 1;
    pthread_cond_broadcast(&tp->cond);
    pthread_mutex_unlock(&tp->mutex);

    // Wait for all threads to finish
    for (size_t i = 0; i < tp->num_threads; i++) {
        pthread_join(tp->threads[i], NULL);
    }

    // Clean up synchronization objects
    pthread_mutex_destroy(&tp->mutex);
    pthread_cond_destroy(&tp->cond);
} 