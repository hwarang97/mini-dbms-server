#include "job_queue.h"

#include <pthread.h>
#include <stdlib.h>

/* Fixed-capacity ring buffer guarded by one mutex and two condition variables. */
struct job_queue {
    job_t **buffer;
    size_t capacity;
    size_t head;
    size_t tail;
    size_t count;
    int shutdown;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
};

job_queue_t *g_queue = NULL;

/* Allocate queue storage and initialize synchronization primitives. */
job_queue_t *queue_init(size_t capacity)
{
    job_queue_t *q;

    if (capacity == 0) {
        return NULL;
    }

    q = (job_queue_t *)malloc(sizeof(*q));
    if (q == NULL) {
        return NULL;
    }

    q->buffer = (job_t **)calloc(capacity, sizeof(*q->buffer));
    if (q->buffer == NULL) {
        free(q);
        return NULL;
    }

    q->capacity = capacity;
    q->head = 0;
    q->tail = 0;
    q->count = 0;
    q->shutdown = 0;

    if (pthread_mutex_init(&q->mutex, NULL) != 0) {
        free(q->buffer);
        free(q);
        return NULL;
    }

    if (pthread_cond_init(&q->not_empty, NULL) != 0) {
        pthread_mutex_destroy(&q->mutex);
        free(q->buffer);
        free(q);
        return NULL;
    }

    if (pthread_cond_init(&q->not_full, NULL) != 0) {
        pthread_cond_destroy(&q->not_empty);
        pthread_mutex_destroy(&q->mutex);
        free(q->buffer);
        free(q);
        return NULL;
    }

    return q;
}

/* Block while the queue is full, unless shutdown has started. */
int queue_push(job_queue_t *q, job_t *job)
{
    if (q == NULL || job == NULL) {
        return -1;
    }

    pthread_mutex_lock(&q->mutex);

    while (q->count == q->capacity && !q->shutdown) {
        pthread_cond_wait(&q->not_full, &q->mutex);
    }

    if (q->shutdown) {
        pthread_mutex_unlock(&q->mutex);
        return -1;
    }

    q->buffer[q->tail] = job;
    q->tail = (q->tail + 1U) % q->capacity;
    q->count++;

    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

/* Block while the queue is empty, and return NULL only after shutdown drains it. */
job_t *queue_pop(job_queue_t *q)
{
    job_t *job;

    if (q == NULL) {
        return NULL;
    }

    pthread_mutex_lock(&q->mutex);

    while (q->count == 0 && !q->shutdown) {
        pthread_cond_wait(&q->not_empty, &q->mutex);
    }

    if (q->count == 0 && q->shutdown) {
        pthread_mutex_unlock(&q->mutex);
        return NULL;
    }

    job = q->buffer[q->head];
    q->buffer[q->head] = NULL;
    q->head = (q->head + 1U) % q->capacity;
    q->count--;

    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->mutex);
    return job;
}

/* Wake every blocked producer and consumer so they can observe shutdown. */
void queue_shutdown(job_queue_t *q)
{
    if (q == NULL) {
        return;
    }

    pthread_mutex_lock(&q->mutex);
    q->shutdown = 1;
    pthread_cond_broadcast(&q->not_empty);
    pthread_cond_broadcast(&q->not_full);
    pthread_mutex_unlock(&q->mutex);
}

/* Release queue-owned memory after all producers and consumers have stopped. */
void queue_destroy(job_queue_t *q)
{
    if (q == NULL) {
        return;
    }

    pthread_cond_destroy(&q->not_full);
    pthread_cond_destroy(&q->not_empty);
    pthread_mutex_destroy(&q->mutex);
    free(q->buffer);
    free(q);
}
