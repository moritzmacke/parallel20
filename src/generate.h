#ifndef _GENERATE_H_
#define _GENERATE_H_

#include "common.h"
#include "pcg/pcg_basic.h"

enum distribution_type {
  RANDOM = 0,
	UNIFORM = 1,
	SORTED = 2,
	SORTED_REV = 3, 
	SORTED_RAND = 4
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

typedef void (*FP_GENITEM)(void *item, size_t index, size_t total, RANDOMGEN *rgen);

//void *generate_work(size_t item_count, size_t item_size, FP_GENITEM item_gen, uint32_t seed, int pattern);

void *generate_random(size_t length, size_t item_size, FP_GENITEM item_generator, uint64_t seed);

void generate_uint(void *item, size_t index, size_t total, RANDOMGEN *rgen);

void generate_double(void *item, size_t index, size_t total, RANDOMGEN *rgen);

#endif /* _GENERATE_H_ */
