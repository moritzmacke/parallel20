#ifndef _GENERATE_H_
#define _GENERATE_H_

#include "common.h"

enum distribution_type {
  RANDOM = 0,
	UNIFORM = 1,
	SORTED = 2,
	SORTED_REV = 3, 
	SORTED_RAND = 4
};

typedef void (*FP_GENITEM)(void *item, size_t index, uint32_t value);

void *generate_work(size_t item_count, size_t item_size, FP_GENITEM item_gen, uint32_t seed, int pattern);

#endif /* _GENERATE_H_ */
