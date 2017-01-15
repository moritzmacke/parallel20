FLAGS = -Wall -pedantic -O3


qs_omp: base.c sort_omp.c generate.c validate.c
	gcc $(FLAGS) -fopenmp $+ -o $@


.PHONY: all
all: qs_omp

.PHONY: clean
clean: 
	rm -f *.o qs_omp
