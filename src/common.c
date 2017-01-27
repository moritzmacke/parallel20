#include <sys/time.h>
#include "sort.h"


void copy(char *a, char *b, size_t size) {
  char *e = &a[size];
  while(a < e) {
    *b++ = *a++;
  }
}

void swap(char *a, char *b, size_t size) {

  while(size > 0) {
    char tmp = *a;
    *a++ = *b;
    *b++ = tmp;
    size--;
  }
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

void print_val(char *val, ITYPE *type) {
  char buf[32];

  type->print(val, buf, sizeof(buf));
  printf("%s", buf);
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

double get_millis() {
  struct timeval now;
  gettimeofday(&now,NULL);  //error checking?
  double t = now.tv_usec/1000.0;
  t += now.tv_sec*1000;
  return t;
}

//simple type from libc
int32_t random32(int32_t *state) {
    int32_t val = *state;
    val = ((val * 1103515245) + 12345) & 0x7fffffff;
    *state = val;
    return val;
}

//length must be > selectCount, selectCount uneven
void *selectPivotRandom(char *array, ITYPE *type, size_t length, uint32_t selectCount, int32_t *rstate) {
	
    if(selectCount > length) {
        selectCount = (length - 1) | 0x1;
    }

//    printf("Select %d from %lu\n", selectCount, length);

	void **pivots = malloc(selectCount*sizeof(void*));
	uint32_t splits = length/selectCount;
		
	pivots[0] = &array[(random32(rstate)%splits)*type->size];
	
	for(int32_t i=1; i<selectCount; i++) { //selectPivots and ad with insertion sort
		void *candidate;
        if(i < selectCount - 1) {
            candidate = &array[(i*splits+(random32(rstate)%splits))*type->size];
        }
        else {
            candidate = &array[(i*splits+(random32(rstate)%(length-i*splits)))*type->size];
        }
		int32_t j = i-1;
		while(j >= 0 && (type->compare(pivots[j], candidate) > 0)) {
			pivots[j+1] = pivots[j];
			j--;
		}
		pivots[j+1] = candidate;

	}

//    printf("End select\n");
	
	return pivots[selectCount/2];
}
