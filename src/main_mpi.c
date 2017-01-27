#include "generate.h"
#include "validate.h"

#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <unistd.h>
#include <math.h>

// MPI header
#include <mpi.h>

#define MASTER_PROCESSOR	0

// Message tags for MPI_Send/_Receive
#define MESSAGE_INPUT		1
#define MESSAGE_PIVOT		2
#define MESSAGE_DATA		3
#define MESSAGE_ORDER 		4
#define MESSAGE_DATA_LEN	5

/* Depth of the partitioning algorithm */
//static unsigned int level = 0;

static double *pivot;

static unsigned int *input, *output, *data = NULL;
static unsigned int data_len;
static unsigned int rank, p, n;

static double starttime, endtime;

#define MASTER(x) if(rank == 0) { x }
#define SLAVE(x) if(rank != 0) { x }

int cmpfunc(const void * a, const void * b)
{
   return ( *(int*)a - *(int*)b );
}

/* Pivot picking strategy */
//enum pivot_t { MANY_RANDOM, ONE_RANDOM, WORST_CASE };
static int pivot_samples = 5;

/* MPI Error handler:
 * Provides minimal error information, aborts process.
 */
void eh(MPI_Comm *comm, int *err,...)
{
	int err_string_len, err_class;
	static char err_string[128];
	
	MPI_Error_class(*err, &err_class);
	
	MPI_Error_string(err_class, err_string, &err_string_len);
	fprintf(stderr, "%2d: %s\n", rank, err_string);
	
	MPI_Error_string(*err, err_string, &err_string_len);
	fprintf(stderr, "%2d: %s\n", rank, err_string);
	
	MPI_Abort(*comm, *err);
	return;
}

/* Pick pivot element in the data array.
 * Choose 5 elements at random, median is the pivot.
 */
double pick_pivot()
{
	double sum = 0;
    int i;
	if(pivot_samples > 0) {
		for(i = 0; i < pivot_samples; i++) {
			sum += data[rand() % data_len];
		}
		return sum / pivot_samples;
	} else { // WORST_CASE
		return 0;
	}
}

unsigned int reorder_slice(int pivot)
{
	unsigned int left = 0, right = data_len-1;
	unsigned int temp;
	for (left = 0; left != right; left++) {
		if(data[left] > pivot) {
			while(data[right] > pivot) {
				right--;
				if(left == right) {
					return left;
				}
			}
			temp = data[right];
			data[right] = data[left];
			data[left] = temp;
		}
	}
	return left;
}

void qsort_partition()
{
	unsigned int real_pivot = 0, middle;
	unsigned int send_len = 0, receive_len = 0;
	unsigned int partitions = 1;
	unsigned int pPerSection = p;
	
	/* Determines if this processor gets data smaller or bigger than pivot */
	unsigned int order = 0;
	
	/* Rank of the processor this processor will share datasets with */
	unsigned int friend = 0;
	
	while(pPerSection > 1) {

//        printf("Rank %d data len %d\n", rank, data_len);

		/* Every processor computes pivot for its data slice */
		double local_pivot = data_len == 0? NAN : pick_pivot();
	
		/* Master receives pivot from every slave, computes median */
		MASTER(
			double received_pivot;
			for(int k=0; k<partitions; k++) {
				pivot[k] = 0;
				for(int j=k*pPerSection; j<(k+1)*pPerSection; j++) {
					if(j != 0) {
						MPI_Recv(&received_pivot, 1, MPI_DOUBLE, j, MESSAGE_PIVOT, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
					} else {  //MASTER
                        received_pivot = local_pivot;
					}
                    pivot[k] += received_pivot != NAN? received_pivot/pPerSection : 0;
				}
			}
		) SLAVE(
			MPI_Send(&local_pivot, 1, MPI_DOUBLE, MASTER_PROCESSOR, MESSAGE_PIVOT, MPI_COMM_WORLD);
		)
		
		/* Master sends pivot and order to every slave */
		MASTER(
			for(int i = 1; i < p; i++) {
                unsigned int ipivot = (unsigned int) roundl(pivot[i/pPerSection]);
				MPI_Send(&ipivot, 1, MPI_UNSIGNED, i, MESSAGE_PIVOT, MPI_COMM_WORLD);
			}
			real_pivot = (unsigned int) roundl(pivot[0]);
		) SLAVE(
			MPI_Recv(&real_pivot, 1, MPI_UNSIGNED, MASTER_PROCESSOR, MESSAGE_PIVOT, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		)
    
		middle = data_len == 0? 0 : reorder_slice(real_pivot);

		/* All processors share datasets with each other */
		unsigned int section = rank/(pPerSection/2);
		order = section % 2;

		if(order == 0) { // Order = 0 means processor is on the "left" side
			/* Calculate friend processor to share data with */
			friend = rank + pPerSection/2;
			
			/* Send every element bigger than the pivot */
			send_len = data_len - middle;
			
			/* Send "bigger" dataset to friend processor */
			MPI_Send(&send_len, 1, MPI_UNSIGNED, friend, MESSAGE_DATA_LEN, MPI_COMM_WORLD);
			MPI_Send(&data[middle], send_len, MPI_UNSIGNED, friend, MESSAGE_DATA, MPI_COMM_WORLD);
			
			/* Receive "smaller" dataset from friend processor */
			MPI_Recv(&receive_len, 1, MPI_UNSIGNED, friend, MESSAGE_DATA_LEN, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			MPI_Recv(&data[middle], receive_len, MPI_UNSIGNED, friend, MESSAGE_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			
			data_len = middle + receive_len;
		} else {
			friend = rank - pPerSection/2;
			
			/* Send every element smaller than the pivot */
			send_len = middle;
			
			/* Receive "bigger" dataset from friend processor */
			MPI_Recv(&receive_len, 1, MPI_UNSIGNED, friend, MESSAGE_DATA_LEN, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			MPI_Recv(&data[data_len], receive_len, MPI_UNSIGNED, friend, MESSAGE_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			
			/* Send "smaller" dataset to friend processor */
			data_len = data_len + receive_len;
			MPI_Send(&send_len, 1, MPI_UNSIGNED, friend, MESSAGE_DATA_LEN, MPI_COMM_WORLD);
			MPI_Send(data, send_len, MPI_UNSIGNED, friend, MESSAGE_DATA, MPI_COMM_WORLD);	
			memmove(data, &data[send_len], (data_len - send_len) * sizeof(int));
			data_len = data_len - send_len;
		}
		
		pPerSection /= 2;
		partitions *= 2;
	}
}

int is_power_of_2(unsigned int x)
{
	while (((x % 2) == 0) && x > 1) /* While x is even and > 1 */
		x /= 2;
	return (x == 1);
}

int main(int argc, char *argv[])
{
	MPI_Errhandler errh;
	char c;
	
	int seed = time(NULL);
	srand(seed);
	int seed2 = rand();
	
	opterr = 0;
	while((c = getopt(argc, argv, "p:n:")) != -1)
	{
		switch(c) {
			case 'n':
				n = strtol(optarg, NULL, 10);
				break;
			case 'p':
				pivot_samples = strtol(optarg, NULL, 10);
				if(!(pivot_samples <= 1000 && pivot_samples >= 0)) {
					MASTER(fprintf(stderr, "Invalid pivot method.\n");)
					goto End;
				}
				break;
			default:
				break;
		}
	}
	
	if(n == 0) {
		MASTER(fprintf(stderr, "Empty dataset.\n");)
		goto End;
	}


	ITYPE type;
	type.size = sizeof(int);
	type.compare = cmpfunc;
	type.print = print_int;
	
	MPI_Init(&argc,&argv);

	/* Set up error handle */
	MPI_Comm_create_errhandler(eh, &errh);
	MPI_Comm_set_errhandler(MPI_COMM_WORLD, errh);

	/* Find out number of processors and rank */
	MPI_Comm_size(MPI_COMM_WORLD,(int *)&p);
	MPI_Comm_rank(MPI_COMM_WORLD,(int *)&rank);

	MASTER(
		data = generate_sequence(n, sizeof(int), generate_uint, RANDOM, seed2);
		input = data;
		print_vals((char *) input, &type, n);
	)
	
	if(p != 1 && n > 10*p) {
		if(!is_power_of_2(p)) {
			MASTER(fprintf(stderr, "Number of processors has to be power of 2.\n");)
			goto End;
		}
	} else {
		/* Use sequential qsort on master processor if dataset is not big enough */
		MASTER(
			qsort(input, n, sizeof(int), cmpfunc);
		)
		goto End;
	}
	
	/* Memory allocation for buffer, pivot array and data slice */
	pivot = (double *) calloc(p/2, sizeof(double));
	SLAVE( 
		data = (unsigned int *) malloc(n * sizeof(int)); 
	)
	data_len = (rank == p-1 ? n - (n/p) * rank : n/p);
	
	/* Master sends data to all slaves. */
	MASTER(
		for(int i = 1; i < p; i++) {
			MPI_Send(&input[i * (n/p)],(i == p-1 ? n - (n/p) * i : (n/p)),MPI_UNSIGNED,i,MESSAGE_INPUT,MPI_COMM_WORLD);
		}
	) SLAVE(
		MPI_Recv(data, data_len, MPI_UNSIGNED, MASTER_PROCESSOR, MESSAGE_INPUT, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
	)
	
	/* Wait for all processors to finish data allocation and initialization */
	MPI_Barrier(MPI_COMM_WORLD);
	
	MASTER( //Master processor measures time
		starttime = MPI_Wtime();
	)
	
	qsort_partition();
	
	/* Once partitioning is done, call sequential qsort */
	qsort(data, data_len, sizeof(int), cmpfunc);
	
	MASTER(
		unsigned int received_len;
		unsigned int pos = 0;
		output = data;
		pos += data_len;
		
		/* Master reassembles sorted data from slave */
		for(int i = 1; i < p; i++) {
			MPI_Recv(&received_len, 1, MPI_UNSIGNED, i, MESSAGE_DATA_LEN, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			MPI_Recv(&output[pos], received_len, MPI_UNSIGNED, i, MESSAGE_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			pos += received_len;
		}
	/* Slave sends its full (sorted) data slice to master */
	) SLAVE(
		MPI_Send(&data_len, 1, MPI_UNSIGNED, MASTER_PROCESSOR, MESSAGE_DATA_LEN, MPI_COMM_WORLD);
		MPI_Send(data, data_len, MPI_UNSIGNED, MASTER_PROCESSOR, MESSAGE_DATA, MPI_COMM_WORLD);
	)
	

	MASTER(
		endtime = MPI_Wtime();
        double parTime = (endtime - starttime)*1000;
		printf("%0.3f ms\n", parTime);
        printf("Stats: n=%d, p=%d, threads=%d, time=%0.3f\n", n, pivot_samples, p, parTime);
		/*
		printf("Validating...\n");

		unsigned int *test = generate_sequence(n, sizeof(int), generate_uint, RANDOM, seed2);
		qsort(test, n, sizeof(int), cmpfunc);

		if(arraysEqual(output, test, n, sizeof(int), cmpfunc, NULL)) {
			printf("OK\n");
		}
		else {
			fprintf(stderr, "Sorting error!\n");
			//print_vals((char *) input, &type, n);
			//print_vals((char *) test, &type, n);
		}*/
	)

	/* Clean up and finalize */
	End:
		if(data != NULL) free(data);
	
		MPI_Errhandler_free(&errh);
		MPI_Finalize();
		return 0;
}
