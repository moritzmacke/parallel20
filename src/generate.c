
#include <stdlib.h>

#include "generate.h"

uint32_t random_uint32(void *state) {
    return pcg32_random_r((pcg32_random_t *) state);
}

void init_random(RGENPCG *rpcg, uint64_t seed) {

    pcg32_random_t s = PCG32_INITIALIZER;
    s.state ^= seed;
    s.inc ^= seed;
    pcg32_srandom_r(&rpcg->rstate, s.state, s.inc);
    rpcg->rgen.random = random_uint32;
    rpcg->rgen.state = (void *) &rpcg->rstate;
}


void *generate_sequence(size_t length, size_t item_size, FP_GENITEM item_generator, int distribution, uint64_t seed) {

  char *p = (char *) malloc(length*item_size);

  RGENPCG rgen;
  
  if(p != NULL) {

    init_random(&rgen, seed);
  
    size_t i;
    size_t n = length;
  
    switch(distribution) {
      case SORTED:
        for (i=0; i<n; i++) { 
          item_generator(&p[i*item_size], i, n, 0, &rgen.rgen);
        }
        break;
      case SORTED_REV:
        for (i=0; i<n; i++) { 
          item_generator(&p[i*item_size], n-i, n, 0, &rgen.rgen);
        }
        break;
      case MIRROR:
        for (i=0; i<n/2; i++) { 
          item_generator(&p[i*item_size], i, n/2, 0, &rgen.rgen);
        }
        for (; i<n; i++) { 
          item_generator(&p[i*item_size], n-i, n - n/2, 0, &rgen.rgen);
        }
        break;
      case RANDOM:
      default:
        for (i=0; i<n; i++) { 
          item_generator(&p[i*item_size], i, n, 1.0, &rgen.rgen);
        }
        break;
    }

  }
  
  return p;
}

void generate_uint(void *item, size_t index, size_t total, double random_weight, RANDOMGEN *rgen) {
  uint32_t *x = (uint32_t *) item;
  
  double r = (rgen->random(rgen->state)/4294967295.0)*total;
  *x = (uint32_t) (r*random_weight + index*(1.0-random_weight));

}

void generate_double(void *item, size_t index, size_t total, double random_weight, RANDOMGEN *rgen) {
  double *v = (double *) item;
  double r = (rgen->random(rgen->state)/4294967295.0)*total;
  *v = r*random_weight + index*(1.0-random_weight);
}

/*
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

*/
