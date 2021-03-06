INC = src/include
PCG = src/pcg
SRC = src
FLAGS = -Wall -pedantic -std=gnu99 -O3 -I$(INC) -I$(SRC)

vpath %.c $(SRC)
.DEFAULT_GOAL := all

qs_omp: main_omp.c sort_omp.c common.c generate.c validate.c pcg/pcg_basic.c
	gcc $(FLAGS) -fopenmp $+ -o $@

qs_cilk2: main_cilk.c sort_cilk2.c common.c validate.c generate.c pcg/pcg_basic.c
	gcc $(FLAGS) -fcilkplus $+ -o $@

qs_cilk2_seq: main_cilk.c sort_cilk2.c common.c validate.c generate.c pcg/pcg_basic.c
	gcc $(FLAGS) -fcilkplus -D SEQUENTIAL $+ -o $@

qs_cilk0: main_cilk.c sort_cilk0.c common.c validate.c generate.c pcg/pcg_basic.c
	gcc $(FLAGS) -fcilkplus $+ -o $@

qs_cilk1: main_cilk.c sort_cilk1.c common.c validate.c generate.c pcg/pcg_basic.c
	gcc $(FLAGS) -fcilkplus $+ -o $@
	
qs_mpi: main_mpi.c common.c generate.c validate.c pcg/pcg_basic.c
	mpicc $(FLAGS) -o $@ $+ -lm

qs_mpi2: sort_mpi2.c common.c validate.c generate.c pcg/pcg_basic.c
	mpicc $(FLAGS) -o $@ $+ 


.PHONY: all
all: qs_omp qs_cilk0 qs_cilk1 qs_cilk2 qs_cilk2_seq qs_mpi

.PHONY: clean
clean: 
	rm -f *.o qs_omp qs_cilk2 qs_cilk2_seq qs_cilk0 qs_cilk1 qs_mpi
