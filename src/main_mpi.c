#include "generate.h"
#include "validate.h"

#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <unistd.h>
//#include <math.h>

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

/* Pivot picking strategy */
enum pivot_t { MANY_RANDOM, ONE_RANDOM, WORST_CASE };
static enum pivot_t piv_method = MANY_RANDOM;

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
unsigned int pick_pivot()
{
	unsigned int sum = 0, i;
	if(piv_method == MANY_RANDOM) {
		for(i = 0; i < 5; i++) {
			sum += data[rand() % data_len];
		}
		return sum / 5;
	} else if(piv_method == ONE_RANDOM) {
		return  data[rand() % data_len];
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
//	unsigned int i = 0, j = 0, k = 0;

//  unsigned int pPerSection = p;
  unsigned int partitions = 1;
  unsigned int pPerSection = p;
	
	/* Determines if this processor gets data smaller or bigger than pivot */
	unsigned int order = 0;
	
	/* Rank of the processor this processor will share datasets with */
	unsigned int friend = 0;
	
	while(pPerSection > 1) {
		/* Every processor computes pivot for its data slice */
		unsigned int local_pivot = pick_pivot();
	
		/* Master receives pivot from every slave, computes median */
		MASTER(
      unsigned int received_pivot;
      for(int k=0; k<partitions; k++) {
        pivot[k] = 0;
        for(int j=k*pPerSection; j<(k+1)*pPerSection; j++) {
          if(j != 0) {
            MPI_Recv(&received_pivot, 1, MPI_UNSIGNED, j, MESSAGE_PIVOT, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
//            printf("Received %d from %d\n", received_pivot, j);
            pivot[k] += received_pivot;
          }
          else {  //MASTER
//            printf("Received %d from %d\n", local_pivot, 0);
            pivot[k] += local_pivot;
          }
        }
//        printf("Sum %d = %d\n", k, pivot[k]);
        pivot[k] /= pPerSection;
      }
		) SLAVE(
			MPI_Send(&local_pivot, 1, MPI_UNSIGNED, MASTER_PROCESSOR, MESSAGE_PIVOT, MPI_COMM_WORLD);
		)
//		MPI_Barrier(MPI_COMM_WORLD);
		
		/* Master sends pivot and order to every slave */
		MASTER(
			for(int i = 1; i < p; i++) {
				MPI_Send(&pivot[i/pPerSection], 1, MPI_UNSIGNED, i, MESSAGE_PIVOT, MPI_COMM_WORLD);
			}
			real_pivot = pivot[0];
		) SLAVE(
			MPI_Recv(&real_pivot, 1, MPI_UNSIGNED, MASTER_PROCESSOR, MESSAGE_PIVOT, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		)
//		MPI_Barrier(MPI_COMM_WORLD);
				
		middle = reorder_slice(real_pivot);

    printf("Rank %d has pivot %d\n", rank, real_pivot);
		
		/* All processors share datasets with each other */
	
    unsigned int section = rank/(pPerSection/2);
//    printf("Rank %d in section %d\n", rank, section);
    order = section % 2;

		unsigned int received_len = 0;
		if(order == 0) { // Order = 0 means processor is on the "left" side
			/* Calculate friend processor to share data with */
//			friend = rank + (unsigned int) pow(2, ((unsigned int) log2(p)) - (level+1));
      friend = rank + pPerSection/2;
			
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
			//friend = rank - (unsigned int) pow(2, ((unsigned int) log2(p)) - (level+1));
      friend = rank - pPerSection/2;
			
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
		
//		MPI_Barrier(MPI_COMM_WORLD);
//		level++;
    pPerSection /= 2;
    partitions *= 2;

	}
}

int main(int argc, char *argv[])
{
	MPI_Errhandler errh;
	char c;
	unsigned int i;
	unsigned int *output;
	
  int seed = time(NULL);

	srand(seed);

  int seed2 = rand();
	
	opterr = 0;
	while((c = getopt(argc, argv, "n:")) != -1)
	{
		switch(c) {
			case 'n':
				n = strtol(optarg, NULL, 10);
				break;
			case 'p':
				piv_method = strtol(optarg, NULL, 10);
				if(!(piv_method <= 2 && piv_method >= 0)) {
					goto Error;
				}
				break;
			default:
				break;
		}
	}
	
	if(n == 0) {
		goto Error;
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
	MPI_Comm_size(MPI_COMM_WORLD,&p);
	MPI_Comm_rank(MPI_COMM_WORLD,&rank);
	
  //TODO need test here to see if p is power of 2!


	MASTER(
		input = generate_sequence(n, sizeof(int), generate_uint, RANDOM, seed2);
		output = input; //input buffer is also used for output
    print_vals((char *) input, &type, n);
	)
	
	if(p != 1 && n > 10*p) {
		i = 2;
		while(p >= i) {
			i*=2;
		}
		p = i/2;
		if(rank >= p) {
			return 0;
		}
	} else {
		/* Use sequential qsort on master processor if dataset is not big enough */
		MASTER(
			qsort(input, n, sizeof(int), cmpfunc);
		)
		goto End;
	}
	
	/* Memory allocation for buffer, pivot array and data slice */
	buf_size = n;
	pivot = (int *) calloc(p/2, sizeof(int));
	buffer = (int *) malloc(buf_size * sizeof(int)); 
	data = (int *) malloc(buf_size * sizeof(int)); 
	data_len = (rank == p-1 ? n - (n/p) * rank : n/p);
	
	/* Master sends data to all slaves. */
	MASTER(
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
	
	/* Once partitioning is done, call sequential qsort */
	qsort(data, data_len, sizeof(int), cmpfunc);
	
	MASTER(
		unsigned int received_len;
		unsigned int pos = 0;
		memcpy(output, data, data_len * sizeof(int));
		pos += data_len;
		
		/* Master reassembles sorted data from slave */
		for(int i = 1; i < p; i++) {
			MPI_Recv(&received_len, 1, MPI_UNSIGNED, i, MESSAGE_DATA_LEN, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			MPI_Recv(buffer, received_len, MPI_UNSIGNED, i, MESSAGE_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			memcpy(&output[pos], buffer, received_len * sizeof(int));
			pos += received_len;
		}
	/* Slave sends its full (sorted) data slice to master */
	) SLAVE(
		MPI_Send(&data_len, 1, MPI_UNSIGNED, MASTER_PROCESSOR, MESSAGE_DATA_LEN, MPI_COMM_WORLD);
		MPI_Send(data, data_len, MPI_UNSIGNED, MASTER_PROCESSOR, MESSAGE_DATA, MPI_COMM_WORLD);
	)
	

	MASTER(
		endtime = MPI_Wtime();
		printf("%0.3f ms\n", (endtime - starttime)*1000);

    printf("Validating...\n");

    unsigned int *test = generate_sequence(n, sizeof(int), generate_uint, RANDOM, seed2);
    qsort(test, n, sizeof(int), cmpfunc);

    if(arraysEqual(input, test, n, sizeof(int), cmpfunc, NULL)) {
      printf("OK\n");
    }
    else {
      printf("Sorting error!\n");
      print_vals((char *) input, &type, n);
      print_vals((char *) test, &type, n);
    }
	)




	/* Clean up and finalize */
	free(buffer);
	free(data);
	
	End:
		MASTER(
			free(input);
		)
	
		MPI_Errhandler_free(&errh);
		MPI_Finalize();
		return 0;
	
	Error:
		return 1;
}
