#ifndef _GENERATE_H_
#define _GENERATE_H_

#include "common.h"
#include "pcg/pcg_basic.h"

enum distribution_type {
  RANDOM = 0,
  SORTED = 1,
  SORTED_REV = 2, 
  MIRROR = 3
};

typedef uint32_t (*FP_RANDOM)(void *state);

typedef struct _frandgen {
  FP_RANDOM random;
  void *state;
} RANDOMGEN;

typedef struct _randgenpcg {
    RANDOMGEN rgen;
    pcg32_random_t rstate;
} RGENPCG;

typedef void (*FP_GENITEM)(void *item, size_t index, size_t total, double random_weight, RANDOMGEN *rgen);

//void *generate_work(size_t item_count, size_t item_size, FP_GENITEM item_gen, uint32_t seed, int pattern);

void *generate_sequence(size_t length, size_t item_size, FP_GENITEM item_generator, int distribution, uint64_t seed);

void generate_uint(void *item, size_t index, size_t total, double random_weight, RANDOMGEN *rgen);

void generate_double(void *item, size_t index, size_t total, double random_weight, RANDOMGEN *rgen);

#endif /* _GENERATE_H_ */
