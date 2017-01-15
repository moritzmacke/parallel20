#ifndef _VALIDATE_H_
#define _VALIDATE_H_

#include "sort.h"

int isSorted(void *array, ITYPE *type, size_t length);

int arraysEqual(void *_a, void *_b, size_t len, size_t item_size, FP_COMPARE compare, size_t *pos);

int validPartition(void *partedArray, void *originalArray, size_t length, size_t item_size, void *pivot, FP_COMPARE compare);

#endif /* _VALIDATE_H_ */
