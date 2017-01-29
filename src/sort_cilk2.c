#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef SEQUENTIAL
#define cilk_spawn
#define cilk_sync
#else
// cilkplus libraries
#include <cilk/cilk.h>
#include <cilk/cilk_api.h>
#endif

#include "sort.h"

#ifdef SEQUENTIAL
static size_t total_swaps = 0;
static size_t pass1_swaps = 0;
static size_t pass2_swaps = 0;
static size_t qs_calls = 0;
#endif

typedef struct _chunk {
  size_t startIndex;
  size_t count_le;
  size_t partial_pf_le;
  size_t count_total;
  size_t split;
} CHUNK;


static size_t umin(size_t a, size_t b) {
  return a <= b ? a : b;
}


static size_t umax(size_t a, size_t b) {
  return a >= b ? a : b;
}


static void swap_elem(char *a, char *b, size_t size) {

    #ifdef SEQUENTIAL
    total_swaps++;
    #endif


  while(size > 0) {
    char tmp = *a;
    *a++ = *b;
    *b++ = tmp;
    size--;
  }

}


static void memswap(char *a, char *b, size_t length) {
	char *e = &a[length];
	char c;
	while(a < e) {
		c = *a;
		*a++ = *b;
		*b++ = c;
	}
}


//
static size_t findSwapSrc(CHUNK chunks[], uint32_t lo, uint32_t range, uint32_t swapIndex, size_t leftSum) {
	if(range > 1) {
		uint32_t mid = lo + range/2;
        size_t midSum = chunks[mid].partial_pf_le + leftSum;

		if(swapIndex < (chunks[mid].startIndex - midSum)) { //< countGt
			return findSwapSrc(chunks, lo, range/2, swapIndex, leftSum);
		}
		else {
			return findSwapSrc(chunks, lo + range/2, range - range/2, swapIndex, midSum);
		}
	}
	else {
        // countLE = leftSum;

		if(lo % 2 == 1) {
			return swapIndex + leftSum;
		} 
		else {
			return swapIndex + leftSum + chunks[lo].count_le; 
		}
	}
}

static size_t findSwapTgt(CHUNK chunks[], uint32_t lo, uint32_t range, uint32_t swapIndex, size_t leftSum) {
  if(range > 1) {
    uint32_t mid = lo + range/2;
    size_t midSum = chunks[mid].partial_pf_le + leftSum;

    if(swapIndex < midSum) {
      return findSwapTgt(chunks, lo, range/2, swapIndex, leftSum);
    }
    else {
	  	return findSwapTgt(chunks, lo + range/2, range - range/2, swapIndex, midSum);
	  }
  }
  else {
    if(lo % 2 == 1) {
      return chunks[lo].split + (swapIndex - leftSum);
    }
    else {
      return chunks[lo].startIndex + (swapIndex - leftSum);
    }
  }
}

//find prefix sum for chunk in index from partial prefixes
static size_t findPrefix(CHUNK chunks[], uint32_t lo, uint32_t range, size_t leftSum, uint32_t index) {
  if(range > 1) {
    uint32_t mid = lo + range/2;
    size_t midSum = chunks[mid].partial_pf_le + leftSum;

    if(index < mid) {
      return findPrefix(chunks, lo, range/2, leftSum, index);
    }
    else {
      return findPrefix(chunks, mid, range - range/2, midSum, index);
    }
  }
  
  return leftSum;
}

static void partitionPass1(char *array, ITYPE *type, size_t length, CHUNK chunks[], uint32_t loChunk, uint32_t range, uint32_t chunksize, void *pivot) {
	
	if(range > 1) {
	  cilk_spawn partitionPass1(array, type, length, chunks, loChunk, range/2, chunksize, pivot);
	  cilk_spawn partitionPass1(array, type, length, chunks, loChunk + range/2, range - (range/2), chunksize, pivot);
    
      cilk_sync;

	  chunks[loChunk + range].partial_pf_le += chunks[loChunk + (range/2)].partial_pf_le; //careful here to avoid race...
	}
	else {
	  size_t start = loChunk*chunksize;
      size_t chunklen = (start+chunksize < length) ? chunksize : length - start;
			
	  chunks[loChunk].startIndex = start;
      chunks[loChunk].count_total = chunklen;

      size_t step = type->size;
	  long left = start;
	  long right = start + chunklen - 1;

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

    #ifdef SEQUENTIAL
            pass1_swaps++;
    #endif

	        swap_elem(&array[left*step], &array[right*step], step);
            left++;
            right--;
	      }
	    }
	      size_t countLE = right - start + 1;
	      chunks[loChunk+1].partial_pf_le = countLE;
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
    #ifdef SEQUENTIAL
              pass1_swaps++;
    #endif
	          swap_elem(&array[left*step], &array[right*step], step);
              left++;
              right--;
	        }
	      }
	      size_t countLE = start + chunklen - right - 1;
	      chunks[loChunk+1].partial_pf_le = countLE;
	      chunks[loChunk].count_le = countLE;	
	      chunks[loChunk].split = left;
		}
	}
}


static void partitionPass2(char *array, ITYPE *type, size_t length, CHUNK chunks[], uint32_t numChunks, size_t swapIndex, size_t swapCount, size_t targetOffset, uint32_t chunksize) {

    if(swapCount == 0) {
        return;
    }

	if(swapCount > umax(chunksize/2, 1000) /*LIMIT*/) { //swaps to do per proc, set to what?
		cilk_spawn partitionPass2(array, type, length, chunks, numChunks, swapIndex, swapCount/2, targetOffset, chunksize);
		cilk_spawn partitionPass2(array, type, length, chunks, numChunks, swapIndex + swapCount/2, swapCount - swapCount/2, targetOffset, chunksize);
	}
	else {
	
		size_t srcIndex = findSwapSrc(chunks, 0, numChunks, swapIndex, 0);
		size_t tgtIndex = findSwapTgt(chunks, 0, numChunks, swapIndex+targetOffset, 0);
		
		uint32_t iSrcRunEnd = (srcIndex/chunksize) | 0x1;	//always ends in uneven block except last, but that one can't be source
		size_t srcRunEnd = chunks[iSrcRunEnd].split;

		uint32_t iTgtRunEnd = (tgtIndex/chunksize) + ((tgtIndex/chunksize)%2);
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

#ifdef SEQUENTIAL
          pass2_swaps += swapLen;
#endif
			memswap(&array[srcIndex*type->size], &array[tgtIndex*type->size], swapLen * type->size);     

			srcIndex += swapLen;
			tgtIndex += swapLen;
			swapCount -= swapLen;
		}
	}
}

static size_t parallelPartition(void *array, ITYPE *type, size_t length, uint32_t chunksize, void *pivot) {

	uint32_t numChunks = (length + chunksize - 1)/chunksize;
	
	CHUNK *chunks = malloc((numChunks+1)*sizeof(CHUNK));

    chunks[0].partial_pf_le = 0;

	partitionPass1((char *) array, type, length, chunks, 0, numChunks, chunksize, pivot);

	chunks[numChunks].startIndex = length;
    chunks[numChunks].count_total = 0;
	chunks[numChunks].split = length;

	size_t countLE = chunks[numChunks].partial_pf_le; //complete for last block

	if(countLE < length && countLE > 0) {	//all le or all gt?
		uint32_t iSplitChunk = countLE/chunksize;
		size_t splitChunkOffset = countLE - iSplitChunk*chunksize;
		CHUNK *splitChunk = &chunks[iSplitChunk];
		
		size_t swapCount = chunksize*iSplitChunk - findPrefix(chunks, 0, numChunks, 0, iSplitChunk);
		
		if(iSplitChunk % 2 == 1) {
			swapCount += umin(splitChunk->split - splitChunk->startIndex, splitChunkOffset);
		}
		else if(splitChunkOffset > splitChunk->count_le) {
			swapCount += splitChunkOffset - splitChunk->count_le;
		}

		partitionPass2((char *) array, type, length, chunks, numChunks, 0, swapCount, countLE-swapCount, chunksize);
	}

  free(chunks);

  return countLE;
}

static void qs(void *array, ITYPE *type, size_t length, uint32_t chunksize, int32_t rstate) {

#ifdef SEQUENTIAL
    qs_calls++;
#endif

  if(length > 2*chunksize) {
    char *a = (char *) array;

    void *pivot = selectPivotRandom(array, type, length, 7, &rstate);
    swap_elem(&a[0], (char *) pivot, type->size); //swap pivot to beginning
    pivot = array;

    //printf("Pivot: %d\n", *pivot);

    size_t size_left = parallelPartition(&a[type->size], type, length-1, chunksize, pivot);
    size_t size_right = length - size_left - 1;

    swap_elem((char *) pivot, &a[size_left*type->size], type->size); //swap pivot in place

    cilk_spawn qs(array, type, size_left, chunksize, rstate);
    cilk_spawn qs(&a[(size_left+1)*type->size], type, size_right, chunksize, rstate);
  }
  else {
    qsort(array, length, type->size, type->compare);
  }
}

void parallelQuicksort(void *array, ITYPE *type, size_t length, size_t chunksize) {
  
  qs(array, type, length, chunksize, 612345789);
#ifdef SEQUENTIAL
  printf("Total swaps: %lu, pass1: %lu, pass2: %lu\n", total_swaps, pass1_swaps, pass2_swaps);
  printf("Quicksort calls: %lu\n", qs_calls);
#endif
}
