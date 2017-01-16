/* Jesper Larsson Traff, TUW, November 2014, November 2016 */
/* CilkPlus with gcc */

// cilk_for not supported before gcc 5.0

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <sys/time.h>

#ifdef SEQUENTIAL
#define cilk_spawn
#define cilk_sync
#else
// cilkplus libraries
#include <cilk/cilk.h>
#include <cilk/cilk_api.h>
#endif

#include "sort.h"
#include "generate.h"
#include "validate.h"


typedef struct _chunk {
  size_t startIndex;
  size_t count_le;
  size_t prefix_le;
  size_t count_total;
  size_t split;
} CHUNK;


size_t umin(size_t a, size_t b) {
  return a <= b ? a : b;
}

size_t umax(size_t a, size_t b) {
  return a >= b ? a : b;
}

double get_millis() {
  struct timeval now;
  gettimeofday(&now, NULL);  //error checking?
  double t = now.tv_usec/1000.0;
  t += now.tv_sec*1000;
  return t;
}

int compare_ints(const void *a, const void *b) {
  const int *i_a = (const int *) a;
  const int *i_b = (const int *) b;
  
  return *i_a - *i_b;
}

int compare_doubles(const void *a, const void *b) {
  const double *da = (const double *) a;
  const double *db = (const double *) b;
  
  return (*da > *db) - (*da < *db);
}

void print_int(void *value, char *target, size_t maxChars) {
  snprintf(target, maxChars, "%u", *((uint32_t *)value));
}

void print_double(void *value, char *target, size_t maxChars) {
  snprintf(target, maxChars, "%f", *((double *)value));
}

void print_vals(char *array, ITYPE *type, size_t length) {
  char buf[32];

  if(length <= 100 && length > 0) {
    int i;
    for(i=0; (i+1)<length; i++) {
      type->print(&array[i*type->size], buf, sizeof(buf));
      printf("%s, ", buf);
    }
    type->print(&array[i*type->size], buf, sizeof(buf));
    printf("%s\n", buf);
  }
}



void print_chunks(CHUNK chunks[], int len) {
  if(len <= 100 && len > 0) {
    int i;
    for(i=0; (i)<len; i++) {
      printf("<%d: [%lu:%lu] LE: %lu pf: %lu split: %lu>, ", i, chunks[i].startIndex, chunks[i].startIndex + chunks[i].count_total, chunks[i].count_le, chunks[i].prefix_le, chunks[i].split);
    }
    printf("<%d: [%lu:%lu] LE: %lu pf: %lu split: %lu> \n", i, chunks[i].startIndex, chunks[i].startIndex + chunks[i].count_total, chunks[i].count_le, chunks[i].prefix_le, chunks[i].split);
  }
}

int same(void *_a, void *_b, size_t len, size_t *pos) {
  char *a = (char *) _a;
  char *b = (char *) _b;

  while(len > 0 && *a++ == *b++) {
    len--;
  }

  *pos = a - (char *) _a;

  return len == 0;
}

void swap(char *a, char *b, size_t size) {

  while(size > 0) {
    char tmp = *a;
    *a++ = *b;
    *b++ = tmp;
    size--;
  }
}


void memswap(char *a, char *b, size_t length) {
	char *e = &a[length];
	char c;
	while(a < e) {
		c = *a;
		*a++ = *b;
		*b++ = c;
	}
}

size_t findSwapSrc(CHUNK chunks[], size_t lo, size_t range, size_t swapIndex) {
	if(range > 1) {
		size_t mid = lo + range/2;
		size_t countGT = chunks[mid].startIndex - chunks[mid].prefix_le;

		if(swapIndex < countGT) {
			return findSwapSrc(chunks, lo, range/2, swapIndex);
		}
		else {
			return findSwapSrc(chunks, lo + range/2, range - range/2, swapIndex);
		}
	}
	else {
		if(lo % 2 == 1) {
			return swapIndex + chunks[lo].prefix_le;
		} 
		else {
			return swapIndex + chunks[lo+1].prefix_le; 
		}
	}
}

size_t findSwapTgt(CHUNK chunks[], size_t lo, size_t range, size_t swapIndex) {
	if(range > 1) {
		size_t mid = lo + range/2;

		if(swapIndex < chunks[mid].prefix_le) {
			return findSwapTgt(chunks, lo, range/2, swapIndex);
		}
		else {
			return findSwapTgt(chunks, lo + range/2, range - range/2, swapIndex);
		}
	}
	else {
		if(lo % 2 == 1) {
			return chunks[lo].split + (swapIndex - chunks[lo].prefix_le);
		}
		else {
			return chunks[lo].startIndex + (swapIndex - chunks[lo].prefix_le);
		}
	}
}

void partitionPass1(char *array, ITYPE *type, size_t length, CHUNK chunks[], size_t loChunk, size_t range, size_t chunksize, void *pivot) {
	
	if(range > 1) {
		cilk_spawn partitionPass1(array, type, length, chunks, loChunk, range/2, chunksize, pivot);
		cilk_spawn partitionPass1(array, type, length, chunks, loChunk + range/2, range - (range/2), chunksize, pivot);
    
    cilk_sync;

		chunks[loChunk + range ].prefix_le += chunks[loChunk + (range/2) ].prefix_le; //careful here to avoid race...
	}
	else {

//    printf("pass1 body\n");

		size_t start = loChunk*chunksize;
    size_t chunklen = (start+chunksize < length) ? chunksize : length - start;
			
		chunks[loChunk].startIndex = start;
    chunks[loChunk].count_total = chunklen;

//    printf("Pass1: start: %lu len: %lu\n", start, chunklen);

    size_t step = type->size;
			
		long left = start;
		long right = start + chunklen - 1;
    
//    char *pstart = &array[start*step];
//    char *pleft = pstart;
//    char *pright = &pstart[right*step];

		if(loChunk % 2 == 0) {
			//le left, gt right
			for(;;) {
				while(left <= right && (type->compare(&array[left*step], pivot) <= 0) ) {
					left++;
				}
				while(right >= left && (type->compare(&array[right*step], pivot) > 0) ) {
					right --;
				}
				if(left > right) {
					break;
				}
				else {
//          printf("Pass1: swap %ld <-> %ld \n", left, right);
					swap(&array[left*step], &array[right*step], step);
          left++;
          right--;
				}
			}

//      right = (pright - (char *) array)/step;

			size_t countLE = right - start + 1;

			chunks[loChunk+1].prefix_le = countLE;
			chunks[loChunk].count_le = countLE;
				
			chunks[loChunk].split = right + 1;
		}
		else {
			//gt left, le right
			for(;;) {
				while(left <= right && (type->compare(&array[left*step], pivot) > 0) ) {
					left++;
				}
				while(right >= left && (type->compare(&array[right*step], pivot) <= 0) ) {
					right --;
				}
				if(left > right) {
					break;
				}
				else {
//          printf("Pass1: swap %ld <-> %ld \n", left, right);
					swap(&array[left*step], &array[right*step], step);
          left++;
          right--;
				}
			}

//      right = (pright - (char *) array)/step;
//      left = (pleft - (char *) array)/step;
				
			size_t countLE = start + chunklen - right - 1;

			chunks[loChunk+1].prefix_le = countLE;
			chunks[loChunk].count_le = countLE;
				
			chunks[loChunk].split = left;
		}
	}
//  cilk_sync;
}

void finishPrefixSums(CHUNK chunks[], size_t loChunk, size_t range, size_t leftSum) {
	if(range > 1) {
		chunks[loChunk + (range/2) ].prefix_le += leftSum;

		finishPrefixSums(chunks, loChunk, range/2, leftSum);
		finishPrefixSums(chunks, loChunk + range/2, range - range/2, chunks[loChunk + (range/2) ].prefix_le);
	}
}

void partitionPass2(char *array, ITYPE *type, size_t length, CHUNK chunks[], size_t numChunks, size_t swapIndex, size_t swapCount, size_t targetOffset, size_t chunksize) {

//  printf("SwapCount: %lu\n", swapCount);

	if(swapCount > chunksize/2 + 1 /*LIMIT*/) { //swaps to do per proc, set to what?
		cilk_spawn partitionPass2(array, type, length, chunks, numChunks, swapIndex, swapCount/2, targetOffset, chunksize);
		cilk_spawn partitionPass2(array, type, length, chunks, numChunks, swapIndex + swapCount/2, swapCount - swapCount/2, targetOffset, chunksize);
    
	}
	else {

		size_t countLE = chunks[numChunks].prefix_le;
		size_t iSplitBlock = countLE/chunksize;
		
		size_t srcIndex = findSwapSrc(chunks, 0, iSplitBlock+1, swapIndex);
		size_t tgtIndex = findSwapTgt(chunks, iSplitBlock, numChunks-iSplitBlock+1, swapIndex+targetOffset);
		
		
		size_t iSrcRunEnd = (srcIndex/chunksize) | 0x1;	//always ends in uneven block except last, but that one can't be source
		size_t srcRunEnd = chunks[iSrcRunEnd].split;

		size_t iTgtRunEnd = (tgtIndex/chunksize) + ((tgtIndex/chunksize)%2);
		size_t tgtRunEnd = (iTgtRunEnd >= numChunks) ? length : chunks[iTgtRunEnd].split; 

		while(swapCount > 0) {
			//find next tgt run
			while (tgtIndex >= tgtRunEnd && iTgtRunEnd+1 < numChunks) {
				tgtIndex = chunks[iTgtRunEnd+1].split;
				iTgtRunEnd += 2;
				//Actually [numChunks] contains length, so..?
				tgtRunEnd = (iTgtRunEnd >= numChunks) ? length : chunks[iTgtRunEnd].split;
			}
			//next src run
			while (srcIndex >= srcRunEnd && iSrcRunEnd+1 < numChunks) {
				srcIndex = chunks[iSrcRunEnd+1].split;
				iSrcRunEnd += 2;
				//could just break if over?
				srcRunEnd = (iSrcRunEnd >= numChunks) ? length : chunks[iSrcRunEnd].split;
			}
			
			size_t srcLen = srcRunEnd - srcIndex;
			size_t tgtLen = tgtRunEnd - tgtIndex;

			size_t swapLen = umin(umin(tgtLen, srcLen), swapCount);
			
			if(swapLen == 0) {
//				printf("Out of swaps?! (Error in swap computation)\n");
				break;
			}
			
//      printf("Swap [%lu:%lu] with [%lu:%lu]\n", srcIndex, srcIndex + swapLen, tgtIndex, tgtIndex + swapLen);
			memswap(&array[srcIndex*type->size], &array[tgtIndex*type->size], swapLen * type->size);	

			srcIndex += swapLen;
			tgtIndex += swapLen;

			
			swapCount -= swapLen;
		}

	}
}

size_t parallelPartition(void *array, ITYPE *type, size_t length, size_t chunksize, int *pivot) {
	
//  printf("Pivot: %d\n", *pivot);

	size_t numChunks = (length + chunksize - 1)/chunksize;
	
	CHUNK *chunks = malloc((numChunks+1)*sizeof(CHUNK));

	//void *a, ITYPE *, size_t length, CHUNK *, size_t loChunk, size_t range, size_t chunksize, void *pivot

  //init first pf
  chunks[0].prefix_le = 0;

//  printf("Before pass1\n");

	partitionPass1((char *) array, type, length, chunks, 0, numChunks, chunksize, pivot);


//  printf("After pass1\n");
//  print_ints(array, length);
	
//	chunks[numChunks].prefix_le += chunks[numChunks-1].prefix_le;
	chunks[numChunks].startIndex = length;
  chunks[numChunks].count_total = 0;
	chunks[numChunks].split = length;
	
	finishPrefixSums(chunks, 0, numChunks, 0);  //O(numChunks)! could be fixed, but works as long as number low ...

//  printf("After prefixSums\n");
//  print_chunks(chunks, numChunks);
	
	size_t countLE = chunks[numChunks].prefix_le;
	if(countLE < length && countLE > 0) {	//all le or all gt?
		size_t iSplitChunk = countLE/chunksize;
		size_t splitChunkOffset = countLE - iSplitChunk*chunksize;
		CHUNK *splitChunk = &chunks[iSplitChunk];
		
		size_t swapCount = chunksize*iSplitChunk - splitChunk->prefix_le;

//    printf("swaps before splitblock: %lu\n", swapCount);
		
		if(iSplitChunk % 2 == 1) {
			swapCount += umin(splitChunk->split - splitChunk->startIndex, splitChunkOffset);
		}
		else if(splitChunkOffset > splitChunk->count_le) {
			swapCount += splitChunkOffset - splitChunk->count_le;
		}

    //divide up swaps
    //void *a, ITYPE *, size_t len, CHUNK *, size_t numChunks, size_t swapIndex, size_t swapCount, size_t targetOffset
		partitionPass2((char *) array, type, length, chunks, numChunks, 0, swapCount, countLE-swapCount, chunksize);
	}
	else {
		//return length?
	}

  free(chunks);

  return countLE;
}

void *selectPivot(void *array, ITYPE *type, size_t length) {
  void *first = array;
  void *mid = (void *) (((char *)array) + (length/2)*type->size);
  void *last = (void *) (((char *)array) + (length-1)*type->size);
  void *pivot = first;

//  printf("Pivot candidates: %d, %d, %d\n", *((int *) first) , *((int *) mid), *((int *) last));

  if(type->compare(first, last) > 0) {
    //first > last
    if(type->compare(first, mid) > 0) {
      //first > last, mid
      if(type->compare(mid, last) > 0) {
        //first > mid > last
        pivot = mid;
      }
      else {
        //first > last >= mid
        pivot = last;
      }
    }
    else {
      //mid >= first > last
    }
  }
  else {
    //last >= first
    if(type->compare(first, mid) > 0) {
      //last >= first > mid
    }
    else {
      //last, mid >= first
      if(type->compare(last, mid) > 0) {
        //last > mid >= first
        pivot = mid;
      }
      else {
        //mid >= last >= first
        pivot = last;
      }
    }
  }

//  printf("Chose: %d\n", *((int *) pivot));

  return pivot;
}

void parallelQuicksort(void *array, ITYPE *type, size_t length, size_t chunksize) {

  
//  printf("size: %lu, %lu chunks\n", size, num_chunks);

//  sleep(1);

  if(length > chunksize) {

//    printf("\n");
//    print_ints(items, size);

    char *a = (char *) array;

    void *pivot = selectPivot(array, type, length);
    swap(&a[0], (char *) pivot, type->size); //swap pivot to beginning
    pivot = (void *) &a[0];

    //printf("Pivot: %d\n", *pivot);

    size_t size_left = parallelPartition(&a[type->size], type, length-1, chunksize, pivot);

    size_t size_right = length - size_left - 1;

//    printf("Split at %lu\n", size_left);


    swap((char *) pivot, &a[size_left*type->size], type->size); //swap pivot in place

//    print_ints(items, size);

    //if(size_left > 0) {
      //cilk_spawn parallelQuicksort(b, a, size_left, chunk_size, depth+1); //balance..., check size before, choose which?
      if(size_left > chunksize) {
        cilk_spawn parallelQuicksort(array, type, size_left, chunksize);
      }
      else {
        parallelQuicksort(array, type, size_left, chunksize);
      }
    //}
    //if(size_right > 0) {
      if(size_right > chunksize) {
        cilk_spawn parallelQuicksort(&a[(size_left+1)*type->size], type, size_right, chunksize);
      }
      else {
        parallelQuicksort(&a[(size_left+1)*type->size], type, size_right, chunksize);
      }


    //}

  }
  else {
    qsort(array, length, type->size, type->compare);
  }

}

int main(int argc, char *argv[])
{
  int i, n;
  void *a;
  void *copy;

  int seed;
  int useDouble = FALSE;
  ITYPE type;

  double start, stop;

  n = 1;
  seed = (int) get_millis();
  int cutoff = 5;
  for (i=1; i<argc&&argv[i][0]=='-'; i++) {
    if (argv[i][1]=='n') i++,sscanf(argv[i],"%d",&n);
    if (argv[i][1]=='s') i++,sscanf(argv[i],"%d",&seed);
    if (argv[i][1]=='c') i++,sscanf(argv[i],"%d",&cutoff);
    if (argv[i][1]=='d') i++, useDouble=TRUE;
  }

//  a = (int*)malloc(n*sizeof(int));

  if(useDouble) {
    type.size = sizeof(double);
    type.compare = compare_doubles;
    type.print = print_double;
    a = generate_random(n, type.size, generate_double, seed);
  }
  else {
    type.size = sizeof(int);
    type.compare = compare_ints;
    type.print = print_int;
    a = generate_random(n, type.size, generate_uint, seed);
  }

 
  copy = malloc(n*type.size);
  memcpy(copy, a, n*type.size);

#ifndef SEQUENTIAL
//  __cilkrts_set_param("nworkers","");
  // check how many workers are available
  int w = __cilkrts_get_nworkers();  
  printf("Total number of workers: %d\n",w);
#endif

  printf("Length: %d (%s), Chunksize: %d\n", n, useDouble? "doubles" : "ints", cutoff);

  print_vals(a, &type, n);

//  int pivot = a[n/2];

  start = get_millis();

//  size_t split = parallelPartition(a, &type, n, cutoff, &pivot);
  parallelQuicksort(a, &type, n, cutoff);
  stop = get_millis();

//  printf("Split point: %lu\n", divide);

  printf("ParQSort time %.3f ms\n",stop-start);

/*
  printf("Split at %lu\n", split);
  print_ints(a, n);

  size_t pos;

  if(validPartition(a, c, n, type.size, &pivot, type.compare, &pos)) {
    printf("OK\n");
  }
  else {
    printf("Partition error at: %lu\n", pos);
  }
*/



  start = get_millis();
  qsort(copy, n, type.size, type.compare);
  stop = get_millis();
  printf("SeqQSort time %.3f ms\n",stop-start);
  print_vals(copy, &type, n);

  // verify
//  for (i=0; i<n-1; i++) assert(a[i]<=a[i+1]);

  size_t pos = 0;

  if(!same(a, copy, type.size*n, &pos)) {
    pos = pos/type.size;
    printf("Sorting error! At %lu \n", pos);
//    printf("%u %u %u\n", pos > 0 ? a[pos-1] : -1, a[pos], a[pos+1]);
//    printf("%u %u %u\n", pos > 0 ? c[pos-1] : -1, c[pos], c[pos+1]);
  }

  free(a);

  free(copy);


  return 0;
}
