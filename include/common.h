#ifndef COMMON_H
#define COMMON_H

typedef struct job_queue job_queue_t;

typedef struct {
    int client_fd; /* worker closes */
    char *sql;     /* worker frees after processing */
} job_t;

extern job_queue_t *g_queue;

#endif /* COMMON_H */
