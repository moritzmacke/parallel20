FLAGS = -Wall -pedantic -O3


qs_omp: base.c sort_omp.c generate.c validate.c pcg/pcg_basic.c
	gcc $(FLAGS) -fopenmp $+ -o $@

qs_cilk2: base_cilk.c sort_cilk2.c validate.c generate.c pcg/pcg_basic.c
	gcc $(FLAGS) -fcilkplus $+ -o $@

qs_cilk0: base_cilk.c sort_cilk0.c validate.c generate.c pcg/pcg_basic.c
	gcc $(FLAGS) -fcilkplus $+ -o $@

qs_cilk1: base_cilk.c sort_cilk1.c validate.c generate.c pcg/pcg_basic.c
	gcc $(FLAGS) -fcilkplus $+ -o $@



.PHONY: all
all: qs_omp

.PHONY: clean
clean: 
	rm -f *.o qs_omp
