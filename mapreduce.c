#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mapreduce.h"

// TODO: concurrent partitioned data structure for intermediate key/value pairs

void MR_Emit(char *key, char *value) {
    // TODO: copy key/value and insert into partition chosen by the partitioner
    (void)key;
    (void)value;
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

void MR_Run(int argc, char *argv[],
            Mapper map, int num_mappers,
            Reducer reduce, int num_reducers,
            Partitioner partition) {
    // TODO:
    //  1. Create num_mappers threads; hand out files argv[1..argc-1]
    //  2. Join mappers
    //  3. Sort each partition by key
    //  4. Create num_reducers threads; call reduce() on each key in order
    //  5. Join reducers and free all memory
    (void)argc;
    (void)argv;
    (void)map;
    (void)num_mappers;
    (void)reduce;
    (void)num_reducers;
    (void)partition;
}
