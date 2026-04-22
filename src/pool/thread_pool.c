#include "thread_pool.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

typedef job_t *(*queue_pop_fn_t)(job_queue_t *queue);
typedef void (*queue_shutdown_fn_t)(job_queue_t *queue);

static pthread_t *g_workers = NULL;
static size_t g_worker_count = 0;
static job_queue_t *g_pool_queue = NULL;
static queue_pop_fn_t g_queue_pop_fn = queue_pop;
static queue_shutdown_fn_t g_queue_shutdown_fn = queue_shutdown;

static void process_job(job_t *job)
{
    if (job == NULL) {
        return;
    }

    if (job->sql != NULL) {
        printf("worker processing SQL: %s\n", job->sql);
    } else {
        printf("worker processing SQL: (null)\n");
    }
    fflush(stdout);

    if (job->client_fd >= 0) {
        close(job->client_fd);
    }

    free(job->sql); /* worker frees after process_job */
    free(job);      /* worker frees after process_job */
}

static void *worker_loop(void *arg)
{
    job_queue_t *queue = (job_queue_t *)arg;
    queue_pop_fn_t pop_fn = g_queue_pop_fn;

    for (;;) {
        job_t *job = pop_fn(queue);

        if (job == NULL) {
            break;
        }

        process_job(job);
    }

    return NULL;
}

int pool_init(size_t num_workers, job_queue_t *queue)
{
    pthread_t *workers;
    size_t created;

    if (num_workers == 0 || queue == NULL || g_workers != NULL) {
        return -1;
    }

    workers = (pthread_t *)calloc(num_workers, sizeof(*workers)); /* pool_destroy frees */
    if (workers == NULL) {
        return -1;
    }

    for (created = 0; created < num_workers; ++created) {
        if (pthread_create(&workers[created], NULL, worker_loop, queue) != 0) {
            size_t i;

            g_queue_shutdown_fn(queue);
            for (i = 0; i < created; ++i) {
                pthread_join(workers[i], NULL);
            }
            free(workers);
            return -1;
        }
    }

    g_workers = workers;
    g_worker_count = num_workers;
    g_pool_queue = queue;

    return 0;
}

void pool_shutdown(void)
{
    size_t i;

    if (g_workers == NULL || g_worker_count == 0) {
        return;
    }

    g_queue_shutdown_fn(g_pool_queue);

    for (i = 0; i < g_worker_count; ++i) {
        pthread_join(g_workers[i], NULL);
    }

    g_worker_count = 0;
}

void pool_destroy(void)
{
    pool_shutdown();

    free(g_workers);
    g_workers = NULL;
    g_pool_queue = NULL;
}

void pool_set_queue_hooks_for_test(job_t *(*pop_fn)(job_queue_t *),
                                   void (*shutdown_fn)(job_queue_t *))
{
    g_queue_pop_fn = (pop_fn != NULL) ? pop_fn : queue_pop;
    g_queue_shutdown_fn = (shutdown_fn != NULL) ? shutdown_fn : queue_shutdown;
}
