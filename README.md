# PA2-Alec-Hayes

COP4600 (Fall 2026), PA#2: **MapReduce**. A single-machine MapReduce library
in C, built with POSIX threads.

## Overview

A user program supplies a `Map()` and a `Reduce()` function and calls
`MR_Run()`. The library:

1. Runs `num_mappers` threads that call `Map()` on each input file
   (`argv[1..argc-1]`).
2. Collects every key/value pair passed to `MR_Emit()` and splits them into
   `num_reducers` partitions using the partition function.
3. Sorts each partition by key.
4. Runs `num_reducers` threads. Each one calls `Reduce()` once per key in its
   partition, in ascending key order. `Reduce()` reads that key's values
   through `get_next()`.

## Repository layout

| Path | Description |
|------|-------------|
| `mapreduce.c` | The library implementation (the graded file) |
| `mapreduce.h` | Interface from the assignment (do not modify) |
| `wordcount.c` | Sample word-count program from the spec, used for testing |
| `Makefile` | Build, valgrind, and packaging targets |
| `deliverables/` | Submission copies: `mapreduce.c` and `PA2.zip` |

## Building and running

Build on Linux or WSL, because the sample program uses `getline` and `strsep`:

```sh
make                          # builds ./wordcount with -Wall -Werror -pthread -O
./wordcount file1.txt file2.txt
make valgrind                 # runs wordcount under valgrind --leak-check=full
make clean
```

## Design

- **Storing emitted pairs:** each partition has its own hash table and its own
  mutex, so mappers writing to different partitions never wait on each other.
  `MR_Emit` copies the key and value, and repeated keys are grouped as they
  arrive, so sorting only has to order distinct keys.
- **Mappers:** a pool of `num_mappers` threads takes files from a shared queue,
  largest file first, which spreads the work evenly across threads.
- **Sorting and reducing:** reducer thread *i* owns partition *i*. It `qsort`s
  that partition's keys, then calls `Reduce()` for each key in order.
  `get_next()` returns the key's values one at a time, then `NULL`.
- **Memory:** each key's memory is freed right after its `Reduce()` call, and
  everything else is freed before `MR_Run` returns. The library is
  valgrind-clean.
