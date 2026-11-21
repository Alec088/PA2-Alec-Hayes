CC = gcc
CFLAGS = -Wall -Werror -pthread -O

all: wordcount

wordcount: wordcount.c mapreduce.c mapreduce.h
	$(CC) $(CFLAGS) -o wordcount wordcount.c mapreduce.c

valgrind: wordcount
	valgrind --leak-check=full ./wordcount wordcount.c

deliverables: deliverables/mapreduce.c

deliverables/mapreduce.c: mapreduce.c
	mkdir -p deliverables
	cp mapreduce.c deliverables/mapreduce.c

zip: deliverables/mapreduce.c
	rm -f deliverables/PA2.zip
	cd deliverables && zip PA2.zip mapreduce.c

clean:
	rm -f wordcount *.o PA2.zip

.PHONY: all valgrind deliverables zip clean
