#ifndef _COMMON_H_
#define _COMMON_H_

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TRUE 1
#define FALSE 0

typedef int (*FP_COMPARE) (const void *, const void *);
typedef void (*FP_PRINT) (void *item, char *target, size_t maxChars);  

typedef struct _itype {
  size_t size;
  FP_COMPARE compare;
  FP_PRINT print;
} ITYPE;

void copy(char *a, char *b, size_t size);
void swap(char *a, char *b, size_t size);

int compare_ints(const void *a, const void *b);
int compare_doubles(const void *a, const void *b);

void print_int(void *value, char *target, size_t maxChars);
void print_double(void *value, char *target, size_t maxChars);

void print_val(char *val, ITYPE *type);
void print_vals(char *array, ITYPE *type, size_t length);

double get_millis();

//simple type from libc
int32_t random32(int32_t *state);
//length must be > selectCount, selectCount uneven
void *selectPivotRandom(char *array, ITYPE *type, size_t length, uint32_t selectCount, int32_t *rstate);

#endif /* _COMMON_H_ */
