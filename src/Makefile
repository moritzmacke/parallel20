FLAGS = -Wall -pedantic -O3


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
	
qs_mpi: main_mpi.c common.c generate.c pcg/pcg_basic.c
	mpicc -lm -o $@ -O3 $+ 

.PHONY: all
all: qs_omp

.PHONY: clean
clean: 
	rm -f *.o qs_omp qs_cilk2 qs_cilk2_seq qs_cilk0 qs_cilk1 qs_mpi
