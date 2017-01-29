#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

// MPI header
#include <mpi.h>

#include "validate.h"
#include "common.h"
#include "generate.h"

struct _opts {
  uint32_t seed;
  uint32_t distribution;
  uint32_t size;
  uint32_t samples;
  int useDouble;
  int longValidate;
};

static size_t umin(size_t a, size_t b) {
  return a <= b ? a : b;
}

//le and gt
size_t partition(char *array, ITYPE *type, size_t length, void *pivot) {
      
    size_t step = type->size;
    char *left = array;
    char *right = &array[(length-1)*step];

    for(;;) {
		  while(left <= right && type->compare(left, pivot) <= 0) {
				left += step;
			}
			while(right >= left && type->compare(right, pivot) > 0) {
				right -= step;
			}
			if(left > right) {
				break;
			}
			else {
				swap(left, right, step);
        left += step;
        right -= step;
			}
    }

		size_t split = (left - array)/step;

    return split;
}

void collectSamples(char *array, ITYPE *type, size_t length, char *buffer, uint32_t selectCount, int32_t *rstate) {

    if(selectCount <= length) {
      for(int i=0; i<selectCount; i++) {
        size_t offset = (i%length)*type->size;
        copy(&array[offset], &buffer[i*type->size], type->size);
      }
    }	
    else {
  	  uint32_t splits = length/selectCount;
  		size_t offset;
      for(int i=0; i<selectCount; i++) {
        if(i < selectCount - 1) {
          offset = (i*splits + (random32(rstate)%splits)) * type->size;
        }
        else {
          offset = (i*splits + (random32(rstate)%(length - i*splits))) * type->size;
        }
        copy(&array[offset], &buffer[i*type->size], type->size);
      }
    }
}

void parallelSelectPivot(void *localSamples, void *pivot, ITYPE *type, int samples, int rank, int size, MPI_Comm comm) {
  
  int pivots[size*samples];

  MPI_Allgather(localSamples, samples, MPI_INT, pivots, samples, MPI_INT, comm);
  
  qsort(pivots, size*samples, type->size, type->compare);

  copy((char *) &pivots[(size*samples)/2], pivot, type->size);
}


void *parallelPartition(char *local, ITYPE *type, int *local_length, size_t total_length, size_t perNode, int samplesPerNode, int32_t rstate, MPI_Comm comm) {

    int comRank, comSize;

    MPI_Comm_size(comm,&comSize);
    MPI_Comm_rank(comm,&comRank);

    while(comSize > 1) {

      char pivotSpace[type->size];
      void *pivot = (void *) &pivotSpace[0];

      if(samplesPerNode == 0) {

        //Select pivot and broadcast it
        if(comRank == 0) {
          void *t = selectPivotRandom(local, type, *local_length, 1, &rstate);
          copy(t, pivot, type->size);
        }
        MPI_Bcast(pivot, 1, MPI_INT, 0, comm);
      }
      else {

        int localSamples[samplesPerNode];
        collectSamples(local, type, *local_length, (char *) localSamples, samplesPerNode, &rstate);

        parallelSelectPivot(localSamples, pivot, type, samplesPerNode, comRank, comSize, comm);
      }      
    
      size_t countLE = partition(local, type, *local_length, pivot);
      size_t countGT = *local_length - countLE; //

      uint64_t prefixSumLE = countLE;

      //Calculate prefix sum    

      MPI_Scan(MPI_IN_PLACE, &prefixSumLE, 1, MPI_UINT64_T, MPI_SUM, comm);

      //last rank has total sum
      uint64_t totalLE = prefixSumLE;
      MPI_Bcast(&totalLE, 1, MPI_UINT64_T, comSize - 1, comm);

      prefixSumLE -= countLE;
      size_t prefixSumGT = comRank*perNode - prefixSumLE;
      uint64_t totalGT = total_length - totalLE;  //

      //Find out how to split up
      double split = totalLE/(double)total_length;
      int leftNodes = (split*comSize) + 0.5;
      if(leftNodes >= comSize) leftNodes = comSize - 1;
      if(leftNodes == 0) leftNodes = 1;
      int rightNodes = comSize - leftNodes;
      
      int perLeftNode = totalLE/leftNodes;
      int perRightNode = totalGT/rightNodes;

      if(comRank == 0) {
          printf("[%.3f] split: %2f Left: %d(%d/node) Right: %d(%d/node) \n", MPI_Wtime(), split, leftNodes, perLeftNode, rightNodes, perRightNode);
      }


      //Prepare for sending
      int sendcounts[comSize];
      int recvcounts[comSize];

      int senddisp[comSize];
      int recvdisp[comSize];
     
      memset(sendcounts, 0, comSize*sizeof(int));
      memset(senddisp, 0, comSize*sizeof(int));
      memset(recvdisp, 0, comSize*sizeof(int));

      size_t leftToSendLE = countLE;
      size_t leftToSendGT = countGT;

      size_t tgtOffset = prefixSumLE;
      int tgtNode = perLeftNode > 0? tgtOffset/perLeftNode : 0;
      size_t capacity = (tgtNode == leftNodes - 1) ? totalLE - tgtNode*perLeftNode : perLeftNode;
      capacity -= (tgtOffset - tgtNode*perLeftNode);
      while(leftToSendLE > 0) {
          int sendLen = umin(capacity, leftToSendLE);
          sendcounts[tgtNode] = sendLen;
          senddisp[tgtNode] = countLE - leftToSendLE;
          leftToSendLE -= sendLen;

          tgtNode++;
          capacity = (tgtNode == leftNodes - 1) ? totalLE - tgtNode*perLeftNode : perLeftNode;
      }

      tgtOffset = prefixSumGT;
      tgtNode = perRightNode > 0? tgtOffset/perRightNode : 0;
      capacity = (tgtNode == rightNodes - 1) ? totalGT - tgtNode*perRightNode : perRightNode;
      capacity -= (tgtOffset - tgtNode*perRightNode);
      while(leftToSendGT > 0) {
          int sendLen = umin(capacity, leftToSendGT);
          sendcounts[leftNodes + tgtNode] = sendLen;
          senddisp[leftNodes + tgtNode] = countLE + (countGT - leftToSendGT);
          leftToSendGT -= sendLen;
          tgtNode++;
          capacity = (tgtNode == rightNodes - 1) ? totalGT - tgtNode*perRightNode : perRightNode;
      }
      
      //Notify others how much they should expect
      MPI_Alltoall(sendcounts, 1, MPI_INT, recvcounts, 1, MPI_INT, comm);
      
      //Prepare for receipt
      int recvOffset = 0;
//      printf("Rank %d receives: ", comRank);
      for(int i=0; i<comSize; i++) {
          recvdisp[i] = recvOffset;
//          printf("%d from %d to [%d], ", recvcounts[i], i, recvOffset);
          recvOffset += recvcounts[i];
      }
//      printf("\n");

      size_t newLength;

      if(comRank < leftNodes) {
          newLength = (comRank == leftNodes - 1)? totalLE - (leftNodes - 1)*perLeftNode : perLeftNode;
          perNode = perLeftNode;
          total_length = totalLE;
      }
      else {
          newLength = (comRank == comSize - 1)? totalGT - (rightNodes - 1)*perRightNode : perRightNode;
          perNode = perRightNode;
          total_length = totalGT;
      }

//      printf("Rank %d has new length %lu\n", comRank, newLength);

      void *buffer = malloc(newLength*type->size);

      //Send & recieve data
      MPI_Alltoallv(local, sendcounts, senddisp, MPI_INT, buffer, recvcounts, recvdisp, MPI_INT, comm);

      *local_length = newLength;
      free(local);
      local = (char *) buffer;


      //Split up into two different communicators
      MPI_Comm newComm;
      MPI_Comm_split(comm, comRank < leftNodes? 0 : 1, comRank, &newComm);

      MPI_Comm_free(&comm);
      comm = newComm;

      MPI_Comm_rank(comm, &comRank);
      MPI_Comm_size(comm, &comSize);

    }

//    printf("%d all alone and done with partitioning, %d elems\n", originalRank, *local_length);
    qsort(local, *local_length, type->size, type->compare);

    MPI_Comm_free(&comm);

    return local;
}


static void print_usage(void);

static int get_options(int argc, char *argv[], struct _opts *options);

int main(int argc, char *argv[])
{
  int rank, size;

  int local_length;

  int perNode;
  uint64_t total_length;
  void *a = NULL;
  void *local;

  ITYPE type;

  double start = 0, stop = 0;
  double parTime = 0;

  struct _opts opts;

  opts.seed = 0;
  opts.distribution = RANDOM;
  opts.size = 100;
  opts.samples = 11;
  opts.useDouble = FALSE;
  opts.longValidate = FALSE;

  if(!get_options(argc, argv, &opts)) {
    return 1;
  }

  srand(opts.seed);

  type.size = sizeof(int);
  type.compare = compare_ints;
  type.print = print_int;

  MPI_Init(&argc,&argv);

  // get rank and size from communicator
  MPI_Comm_size(MPI_COMM_WORLD,&size);
  MPI_Comm_rank(MPI_COMM_WORLD,&rank);

  total_length = opts.size;
  perNode = total_length/size;
  local_length = (rank == size - 1)? total_length - rank*perNode : perNode;

  local = malloc(type.size*local_length);

  if (rank==0) {

    a = generate_sequence(total_length, type.size, generate_uint, opts.distribution, opts.seed);

    print_vals(a, &type, total_length);

    MPI_Scatter(a, perNode, MPI_INT, local, perNode, MPI_INT, 0, MPI_COMM_WORLD);

    int sent = size*perNode;
    char *t = (char *) a;
    MPI_Send(&t[sent*type.size], total_length - sent, MPI_INT, size - 1, 0, MPI_COMM_WORLD);

    free(a);
  } else {
    MPI_Scatter(NULL, 0, 0, local, perNode, MPI_INT, 0, MPI_COMM_WORLD);
    if(rank == size - 1) {
      int rest = local_length - perNode;
      char *t = (char *) local;
      MPI_Recv(&t[perNode*type.size], rest, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }
  }

  MPI_Comm firstComm;
  MPI_Comm_dup(MPI_COMM_WORLD, &firstComm);
  
  if(rank == 0) {
    start = MPI_Wtime();
  }

  //do work

  local = parallelPartition(local, &type, &local_length, total_length, perNode, opts.samples, opts.seed + rank, firstComm);
    
  //

  if(rank==0) {

    a = malloc(total_length*type.size);

    int recvcounts[size];
    int recvdisp[size];
    MPI_Gather(&local_length, 1, MPI_INT, recvcounts, 1, MPI_INT, 0, MPI_COMM_WORLD);
    
    int offset = 0;
    for(int i=0; i<size; i++) {
        recvdisp[i] = offset;
        offset += recvcounts[i];
        printf("R%03d returns %d%s", i, recvcounts[i], (i < size-1) ? ", " : "");
    }
    printf("\n");

    MPI_Gatherv(local, local_length, MPI_INT, a, recvcounts, recvdisp, MPI_INT, 0, MPI_COMM_WORLD);

    stop = MPI_Wtime();
    parTime = stop - start;
  }
  else {
    MPI_Gather(&local_length, 1, MPI_INT, NULL, 0, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Gatherv(local, local_length, MPI_INT, NULL, NULL, NULL, MPI_INT, 0, MPI_COMM_WORLD);
  }

      
  MPI_Finalize();
  
  int ok = 0;
  if(rank ==0) {

    printf("Validating...\n");
    if(opts.longValidate) {
      void *copy = generate_sequence(total_length, type.size, generate_uint, opts.distribution, opts.seed);
      
      start = MPI_Wtime();
      qsort(copy, total_length, type.size, type.compare);
      stop = MPI_Wtime();
      printf("SeqSort time %3f ms\n", (stop-start)*1000);

      if(!arraysEqual(a, copy, total_length, type.size, type.compare, NULL)) {
        printf("Sorting error.\n");
      }
      else {
        ok = 1;
      }
      free(copy);
    }
    else {
      if(!isSorted(a, &type, total_length)) {
        printf("Sorting error! \n");
      }
      else {
        ok = 1;
      }
    }

    //print_vals(a, &type, total_length);
    printf("MPI time %.3f ms\n", parTime*1000);
    printf("Stats: n=%lu, p=%d, threads=%d, seed=%d, time=%0.3f, status=%s\n", total_length, opts.samples, size, opts.seed, parTime*1000, ok ? "OK" : "ERR");
    free(a);
  }

  free(local);


  return 0;
}

void print_usage(void) {
  printf(" ");
}

int get_options(int argc, char *argv[], struct _opts *options) {

  int success = TRUE;
  int c;


  while ((c = getopt(argc, argv, "n:s:g:p:dv")) != -1) {
    switch (c) {
      case 'n':
        options->size = (uint32_t) strtol(optarg, NULL, 0);
        break;
      case 's':
        options->seed = (uint32_t) strtol(optarg, NULL, 0);
        break;
      case 'g':
        options->distribution = (uint32_t) strtol(optarg, NULL, 0);
        break;
      case 'p':
        options->samples = (uint32_t) strtol(optarg, NULL, 0);
        break;
      case 'd':
        options->useDouble = TRUE;
        break;
      case 'v':
        options->longValidate = TRUE;
        break;
      case '?':
        success = FALSE;
        if (optopt == 'c') {
          printf("Option -%c requires an argument.\n", optopt);
        }
        else {
          printf("Unknown option '-%c'.\n", optopt);
        }
        print_usage();
        break;
      default:
        break;
    }
  }

  return success;

}

