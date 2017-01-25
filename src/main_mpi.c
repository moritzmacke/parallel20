#include "generate.h"

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

static unsigned int level = 0;
static unsigned int *pivot;
static unsigned int *buffer, *input, *data;
static unsigned int buf_len, buf_size, data_len;
static unsigned int rank, p, n;

static double starttime, endtime;

#define MASTER(x) if(rank == 0) { x }
#define SLAVE(x) if(rank != 0) { x }

int cmpfunc(const void * a, const void * b)
{
   return ( *(int*)a - *(int*)b );
}

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
int pick_pivot()
{
	unsigned int sum = 0, i;
	for(i = 0; i < 5; i++) {
		sum += data[rand() % data_len];
	}
	return sum / 5;
}

int reorder_slice(int pivot)
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
	unsigned int i = 0, j = 0, k = 0;
	unsigned int order = 0;
	unsigned int friend = 0;
	
	while(level < (unsigned int) log2(p)) {
		/* Every processor computes pivot for its data slice */
		unsigned int local_pivot = pick_pivot();
	
		/* Master receives pivot from every slave, computes median */
		MASTER(
			j = 0;
			unsigned int received_pivot;
			for(i = 1; i < p; i++) {
				MPI_Recv(&received_pivot, 1, MPI_UNSIGNED, i, MESSAGE_PIVOT, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				pivot[j] += received_pivot;
				if((i+1) % (p / ((unsigned int) pow(2, level))) == 0) {
					j++;
				}
			}
			pivot[0] += local_pivot;
			for(k = 0; k <= j; k++) {
				pivot[k] /= (p / ((unsigned int) pow(2, level)));
			}
		) SLAVE(
			MPI_Send(&local_pivot, 1, MPI_UNSIGNED, MASTER_PROCESSOR, MESSAGE_PIVOT, MPI_COMM_WORLD);
		)
		MPI_Barrier(MPI_COMM_WORLD);
		
		/* Master sends pivot and order to every slave */
		MASTER(
			j = 0;
			unsigned int c = 1;
			unsigned int slave_order = 0;
			for(i = 1; i < p; i++, c++) {
				MPI_Send(&pivot[j], 1, MPI_UNSIGNED, i, MESSAGE_PIVOT, MPI_COMM_WORLD);
				if((i+1) % (p / ((unsigned int) pow(2, level))) == 0) {
					j++;
				}
				if(c >= p / ((unsigned int)pow(2, level+1))) {
					c = 0;
					if(slave_order == 1) slave_order = 0;
					else slave_order = 1;
				}
				MPI_Send(&slave_order, 1, MPI_UNSIGNED, i, MESSAGE_ORDER, MPI_COMM_WORLD);
			}
			real_pivot = pivot[0];
		) SLAVE(
			MPI_Recv(&real_pivot, 1, MPI_UNSIGNED, MASTER_PROCESSOR, MESSAGE_PIVOT, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			MPI_Recv(&order, 1, MPI_UNSIGNED, MASTER_PROCESSOR, MESSAGE_ORDER, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		)
		MPI_Barrier(MPI_COMM_WORLD);
				
		middle = reorder_slice(real_pivot);
		
		/* All processors share datasets with each other */
	
		unsigned int received_len = 0;
		if(order == 0) { // Order = 0 means processor is on the "left" side
			/* Calculate friend processor to share data with */
			friend = rank + (unsigned int) pow(2, ((unsigned int) log2(p)) - (level+1));
			
			/* Send every element bigger than the pivot */
			buf_len = data_len - middle;
			
			/* Send "bigger" dataset to friend processor */
			memcpy(buffer, &data[middle], buf_len * sizeof(int));
			MPI_Send(&buf_len, 1, MPI_UNSIGNED, friend, MESSAGE_DATA_LEN, MPI_COMM_WORLD);
			MPI_Send(buffer, buf_len, MPI_UNSIGNED, friend, MESSAGE_DATA, MPI_COMM_WORLD);
			
			/* Receive "smaller" dataset from friend processor */
			MPI_Recv(&received_len, 1, MPI_UNSIGNED, friend, MESSAGE_DATA_LEN, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			MPI_Recv(buffer, received_len, MPI_UNSIGNED, friend, MESSAGE_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			
			memcpy(&data[middle], buffer, received_len * sizeof(int));
			data_len = middle + received_len;
		} else {
			friend = rank - (unsigned int) pow(2, ((unsigned int) log2(p)) - (level+1));
			
			/* Send every element smaller than the pivot */
			buf_len = middle;
			
			/* Receive "bigger" dataset from friend processor */
			MPI_Recv(&received_len, 1, MPI_UNSIGNED, friend, MESSAGE_DATA_LEN, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			MPI_Recv(buffer, received_len, MPI_UNSIGNED, friend, MESSAGE_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			
			/* Send "smaller" dataset to friend processor */
			memcpy(&data[data_len], buffer, received_len * sizeof(int)); //Append received buffer first, then cut data from front and move back to index 0
			data_len = data_len + received_len;
			memcpy(buffer, data, buf_len * sizeof(int));
			memmove(data, &data[buf_len], (data_len - buf_len) * sizeof(int));
			data_len = data_len - buf_len;
			MPI_Send(&buf_len, 1, MPI_UNSIGNED, friend, MESSAGE_DATA_LEN, MPI_COMM_WORLD);
			MPI_Send(buffer, buf_len, MPI_UNSIGNED, friend, MESSAGE_DATA, MPI_COMM_WORLD);	
		}
		
		MPI_Barrier(MPI_COMM_WORLD);
		level++;
	}
}

int main(int argc, char *argv[])
{
	MPI_Errhandler errh;
	char c;
	unsigned int i;
	unsigned int *output; //input buffer is also used for output
	
	srand(time(NULL));
	
	opterr = 0;
	while((c = getopt(argc, argv, "n:")) != -1)
	{
		switch(c) {
			case 'n':
				n = strtol(optarg, NULL, 10);
				break;
			default:
				break;
		}
	}
	
	if(n == 0) {
		goto Error;
	}
	
	MPI_Init(&argc,&argv);

	/* Set up error handle */
	MPI_Comm_create_errhandler(eh, &errh);
	MPI_Comm_set_errhandler(MPI_COMM_WORLD, errh);

	/* Find out number of processors and rank */
	MPI_Comm_size(MPI_COMM_WORLD,&p);
	MPI_Comm_rank(MPI_COMM_WORLD,&rank);
	
	if(p != 1) {
		i = 2;
		while(p >= i) {
			i*=2;
		}
		p = i/2;
		if(rank >= p) {
			return 0;
		}
	} else {
		//sequential qsort
		goto Error;
	}
	
	/* Memory allocation for buffer, pivot array and data slice */
	buf_size = n;
	pivot = (int *) calloc(p/2, sizeof(int));
	buffer = (int *) malloc(buf_size * sizeof(int)); 
	data = (int *) malloc(buf_size * sizeof(int)); 
	data_len = (rank == p-1 ? n - (n/p) * rank : n/p);
	
	/* Master sends data to all slaves. */
	MASTER(
		input = generate_sequence(n, sizeof(int), generate_uint, RANDOM, rand());
		memcpy(data, input, n/p * sizeof(int));
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
	qsort(data, data_len, sizeof(int), cmpfunc);
	
	MASTER(
		unsigned int received_len;
		unsigned int pos = 0;
		output = input;
		memcpy(output, data, data_len * sizeof(int));
		pos += data_len;
		
		for(int i = 1; i < p; i++) {
			MPI_Recv(&received_len, 1, MPI_UNSIGNED, i, MESSAGE_DATA_LEN, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			MPI_Recv(buffer, received_len, MPI_UNSIGNED, i, MESSAGE_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			memcpy(&output[pos], buffer, received_len * sizeof(int));
			pos += received_len;
		}
	) SLAVE(
		MPI_Send(&data_len, 1, MPI_UNSIGNED, MASTER_PROCESSOR, MESSAGE_DATA_LEN, MPI_COMM_WORLD);
		MPI_Send(data, data_len, MPI_UNSIGNED, MASTER_PROCESSOR, MESSAGE_DATA, MPI_COMM_WORLD);
	)
	
	MASTER(
		for(int i = 0; i < n; i++) {
			printf("%3d ", output[i]);
			if((i+1) % 10 == 0) printf("\n");
		}
	)
	
	/* Master reassembles sorted data from slave */

	MASTER(
		endtime = MPI_Wtime();
		printf("%d", (unsigned int) endtime);
		free(input);
	)

	/* Clean up and finalize */
	free(buffer);
	free(data);
	MPI_Errhandler_free(&errh);
	MPI_Finalize();
	
	return 0;
	
	Error:
	return 1;
}
