
#include "validate.h"

int isSorted(void *array, ITYPE *type, size_t length) {

   size_t i;
   for (i=0; i<length-1; i++) {
     void *a = ((char *) array) + i*type->size;
     void *b = ((char *) a) + type->size;
     if(type->compare(a, b) > 0) {
       return 0;
     }
   }
   
   return 1;
}

int arraysEqual(void *_a, void *_b, size_t len, size_t item_size, FP_COMPARE compare, size_t *pos) {
  char *a = (char *) _a;
  char *b = (char *) _b;

  while(len > 0 && compare((void *) a, (void *) b) == 0) {
    a += item_size;
    b += item_size;
    len--;
  }
  
  if(pos != NULL) {
    *pos = (a - (char *) _a)/item_size;
  }

  return len == 0;
}

int validPartition(void *partedArray, void *originalArray, size_t length, size_t item_size, void *pivot, FP_COMPARE compare, size_t *pos) {
		
    char *p = (char *) partedArray;

		size_t i=0;
		for(; i < length; i++) {
			if(compare((void *) &p[i*item_size], pivot) > 0) {
				break;
			}
		}

    size_t j=i;
		for(; j < length; j++) {
			if(compare((void *) &p[j*item_size], pivot) <= 0) {
        if(pos != NULL) { *pos = j; }
				return FALSE;
			}
		}

    qsort(partedArray, i, item_size, compare);
    qsort(&p[i*item_size], length-i, item_size, compare);
    qsort(originalArray, length, item_size, compare);
	
		return arraysEqual(partedArray, originalArray, length, item_size, compare, pos);

}
