CC = gcc
CFLAGS = -Wall -Werror -pthread -O

all: wordcount

wordcount: wordcount.c mapreduce.c mapreduce.h
	$(CC) $(CFLAGS) -o wordcount wordcount.c mapreduce.c

valgrind: wordcount
	valgrind --leak-check=full ./wordcount wordcount.c

zip:
	zip PA2.zip mapreduce.c

clean:
	rm -f wordcount *.o PA2.zip

.PHONY: all valgrind zip clean
