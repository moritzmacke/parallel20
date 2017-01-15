
#include <stdlib.h>

#include "generate.h"


void *generate_work(size_t item_count, size_t item_size, FP_GENITEM item_gen, uint32_t seed, int pattern) {

  char *p = (char *) malloc(item_count*item_size);
  
  if(p != NULL) {
  
    size_t i;
    size_t n = item_count;
  
    if (pattern==-1) {
      for (i=0; i<n; i++) {
        item_gen(p+i*item_size, i, (100-i)%27);
      }
    } else if (pattern==UNIFORM) {
      for (i=0; i<n; i++) { 
        item_gen(p+i*item_size, i, 27);  // worst-case input
      }
    } else if (pattern == RANDOM) {
      srand(seed);
      for (i=0; i<n; i++) { 
        item_gen(p+i*item_size, i, rand()%n);
      }
    }
    else if (pattern == SORTED) {
      for (i=0; i<n; i++) {
         item_gen(p+i*item_size, i, i);
      }
    }
    else if (pattern == SORTED_RAND) {
      int var = n/100 + 2;
      for (i=0; i<n; i++) {
         item_gen(p+i*item_size, i, i+rand()%var);
      }
    }
  }
  
  return p;
}
