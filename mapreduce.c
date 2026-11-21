/*
 * GenAI Disclosure
 * ----------------
 * This file was generated using Generative AI, per the course GenAI policy.
 *
 * Citation: Claude (Claude Opus 5.5, Anthropic). Accessed 2026-11-20.
 * Prompt: "I am working on this PA2 assignment attached and this is the local folder for it, can you do it and make sure to not include any Claude co-author on commits. You can use PA1 as a reference if you need to but that was for a different assignment." (with the Map Reduce assignment description
 * attached). Generated using Claude.
 *
 * Scope: the entire implementation in this file (data structures, threading,
 * sorting, MR_Emit, MR_Run, get_next, and memory cleanup) was AI-generated.
 * MR_DefaultHashPartition is copied from the assignment handout.
 */

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include "mapreduce.h"

/*
 * Design
 * ------
 * Intermediate data lives in one hash table per partition (num_partitions ==
 * num_reducers). Each table is guarded by its own mutex, so mappers emitting
 * to different partitions never contend. Every distinct key gets one entry
 * holding a growable array of value copies, so duplicate keys are grouped
 * at emit time and the sort phase only has to order distinct keys.
 *
 * Mappers: a pool of num_mappers threads pulls files off a shared queue
 * (largest file first, for better load balance).
 *
 * Reducers: reducer thread i owns partition i. It flattens its table into an
 * array of entries, qsorts by key, then calls Reduce() once per key. The
 * getter walks the current entry's value array. Each partition has exactly one
 * reducer thread, so the getter needs no locking. Memory is freed as each key
 * is reduced.
 */

#define INIT_BUCKETS 1024
#define INIT_VALUES  4

typedef struct entry {
    char *key;
    char **values;
    size_t nvalues;
    size_t cap;
    size_t next;            /* getter cursor */
    unsigned long hash;
    struct entry *chain;
} entry_t;

typedef struct {
    pthread_mutex_t lock;
    entry_t **buckets;
    size_t nbuckets;        /* always a power of two */
    size_t nkeys;
    entry_t *current;       /* key currently being reduced */
} partition_t;

typedef struct {
    char *name;
    off_t size;
} file_task_t;

static partition_t *partitions;
static int num_partitions;
static Partitioner partitioner;

static Mapper mapper_fn;
static file_task_t *files;
static int num_files;
static int next_file;
static pthread_mutex_t file_lock = PTHREAD_MUTEX_INITIALIZER;

static Reducer reducer_fn;

static void *xmalloc(size_t n) {
    void *p = malloc(n);
    if (p == NULL && n != 0) {
        perror("malloc");
        exit(1);
    }
    return p;
}

static void *xrealloc(void *old, size_t n) {
    void *p = realloc(old, n);
    if (p == NULL && n != 0) {
        perror("realloc");
        exit(1);
    }
    return p;
}

static char *xstrdup(const char *s) {
    size_t n = strlen(s) + 1;
    char *p = xmalloc(n);
    memcpy(p, s, n);
    return p;
}

/* FNV-1a; deliberately different from the partitioner's djb2 so that bucket
 * indices are not correlated with the partition a key landed in. */
static unsigned long bucket_hash(const char *s) {
    unsigned long h = 1469598103934665603UL;
    while (*s) {
        h ^= (unsigned char)*s++;
        h *= 1099511628211UL;
    }
    return h;
}

static void table_grow(partition_t *p) {
    size_t newn = p->nbuckets * 2;
    entry_t **nb = calloc(newn, sizeof(entry_t *));
    if (nb == NULL) {
        perror("calloc");
        exit(1);
    }
    for (size_t i = 0; i < p->nbuckets; i++) {
        entry_t *e = p->buckets[i];
        while (e) {
            entry_t *nx = e->chain;
            size_t idx = e->hash & (newn - 1);
            e->chain = nb[idx];
            nb[idx] = e;
            e = nx;
        }
    }
    free(p->buckets);
    p->buckets = nb;
    p->nbuckets = newn;
}

void MR_Emit(char *key, char *value) {
    if (key == NULL || value == NULL || partitions == NULL)
        return;

    unsigned long part = partitioner(key, num_partitions) % (unsigned long)num_partitions;
    partition_t *p = &partitions[part];
    unsigned long h = bucket_hash(key);
    char *vcopy = xstrdup(value);   /* copy outside the lock */

    pthread_mutex_lock(&p->lock);

    size_t idx = h & (p->nbuckets - 1);
    entry_t *e = p->buckets[idx];
    while (e && (e->hash != h || strcmp(e->key, key) != 0))
        e = e->chain;

    if (e == NULL) {
        e = xmalloc(sizeof(*e));
        e->key = xstrdup(key);
        e->cap = INIT_VALUES;
        e->values = xmalloc(e->cap * sizeof(char *));
        e->nvalues = 0;
        e->next = 0;
        e->hash = h;
        e->chain = p->buckets[idx];
        p->buckets[idx] = e;
        if (++p->nkeys > p->nbuckets)
            table_grow(p);
    }

    if (e->nvalues == e->cap) {
        e->cap *= 2;
        e->values = xrealloc(e->values, e->cap * sizeof(char *));
    }
    e->values[e->nvalues++] = vcopy;

    pthread_mutex_unlock(&p->lock);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

static char *get_next(char *key, int partition_number) {
    (void)key;
    if (partition_number < 0 || partition_number >= num_partitions)
        return NULL;
    entry_t *e = partitions[partition_number].current;
    if (e == NULL || e->next >= e->nvalues)
        return NULL;
    return e->values[e->next++];
}

static int cmp_file_desc(const void *a, const void *b) {
    off_t sa = ((const file_task_t *)a)->size;
    off_t sb = ((const file_task_t *)b)->size;
    return (sa < sb) - (sa > sb);
}

static void *map_worker(void *arg) {
    (void)arg;
    for (;;) {
        pthread_mutex_lock(&file_lock);
        int i = next_file++;
        pthread_mutex_unlock(&file_lock);
        if (i >= num_files)
            break;
        mapper_fn(files[i].name);
    }
    return NULL;
}

static int cmp_entry(const void *a, const void *b) {
    return strcmp((*(entry_t *const *)a)->key, (*(entry_t *const *)b)->key);
}

static void *reduce_worker(void *arg) {
    int pn = (int)(long)arg;
    partition_t *p = &partitions[pn];

    entry_t **sorted = xmalloc((p->nkeys ? p->nkeys : 1) * sizeof(entry_t *));
    size_t n = 0;
    for (size_t i = 0; i < p->nbuckets; i++)
        for (entry_t *e = p->buckets[i]; e; e = e->chain)
            sorted[n++] = e;
    free(p->buckets);
    p->buckets = NULL;
    p->nbuckets = 0;

    qsort(sorted, n, sizeof(entry_t *), cmp_entry);

    for (size_t i = 0; i < n; i++) {
        entry_t *e = sorted[i];
        p->current = e;
        reducer_fn(e->key, get_next, pn);
        p->current = NULL;
        for (size_t j = 0; j < e->nvalues; j++)
            free(e->values[j]);
        free(e->values);
        free(e->key);
        free(e);
    }
    free(sorted);
    return NULL;
}

void MR_Run(int argc, char *argv[],
            Mapper map, int num_mappers,
            Reducer reduce, int num_reducers,
            Partitioner partition) {
    if (num_mappers < 1)
        num_mappers = 1;
    if (num_reducers < 1)
        num_reducers = 1;

    mapper_fn = map;
    reducer_fn = reduce;
    partitioner = partition ? partition : MR_DefaultHashPartition;
    num_partitions = num_reducers;

    /* Partition tables */
    partitions = xmalloc(num_partitions * sizeof(partition_t));
    for (int i = 0; i < num_partitions; i++) {
        partition_t *p = &partitions[i];
        pthread_mutex_init(&p->lock, NULL);
        p->nbuckets = INIT_BUCKETS;
        p->buckets = calloc(p->nbuckets, sizeof(entry_t *));
        if (p->buckets == NULL) {
            perror("calloc");
            exit(1);
        }
        p->nkeys = 0;
        p->current = NULL;
    }

    /* File queue, largest first */
    num_files = argc > 1 ? argc - 1 : 0;
    next_file = 0;
    files = xmalloc((num_files ? num_files : 1) * sizeof(file_task_t));
    for (int i = 0; i < num_files; i++) {
        struct stat st;
        files[i].name = argv[i + 1];
        files[i].size = stat(argv[i + 1], &st) == 0 ? st.st_size : 0;
    }
    qsort(files, num_files, sizeof(file_task_t), cmp_file_desc);

    /* Map phase */
    int nm = num_mappers < num_files ? num_mappers : num_files;
    if (nm > 0) {
        pthread_t *mt = xmalloc(nm * sizeof(pthread_t));
        for (int i = 0; i < nm; i++)
            pthread_create(&mt[i], NULL, map_worker, NULL);
        for (int i = 0; i < nm; i++)
            pthread_join(mt[i], NULL);
        free(mt);
    }
    free(files);
    files = NULL;

    /* Sort + reduce phase: one thread per partition */
    pthread_t *rt = xmalloc(num_reducers * sizeof(pthread_t));
    for (int i = 0; i < num_reducers; i++)
        pthread_create(&rt[i], NULL, reduce_worker, (void *)(long)i);
    for (int i = 0; i < num_reducers; i++)
        pthread_join(rt[i], NULL);
    free(rt);

    for (int i = 0; i < num_partitions; i++)
        pthread_mutex_destroy(&partitions[i].lock);
    free(partitions);
    partitions = NULL;
}
