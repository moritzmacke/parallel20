#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>

#include "sort.h"
#include "generate.h"

#define CUTOFF 8

struct _opts {
  uint32_t threads;
  uint32_t seed;
  uint32_t pattern;
  uint32_t size;
};

typedef char * PCHAR;

typedef struct _pstack {
  PCHAR *sp;
  PCHAR *base;
  size_t size;
} PSTACK;


void pstack_init(PSTACK *stack, char **array, size_t size) {
  stack->sp = stack->base = array;
  stack->size = size;
}

void pstack_push(char *p, PSTACK *stack) {
  *(stack->sp++) = p; //no OF check yet...
}


char *pstack_pop(PSTACK *stack) {
  return *(--stack->sp); //no OF check yet...
}

int pstack_empty(PSTACK *stack) {
  return stack->sp <= stack->base;
}

void swap(char *a, char *b, size_t size) {

  while(size > 0) {
    char tmp = *a;
    *a++ = *b;
    *b++ = tmp;
    size--;
  }
}


void generate_item(void *item, size_t index, uint32_t value) {
  int *x = (int *) item;
  *x = value;
}

int compare_ints(const void *a, const void *b) {
  const int *i_a = (const int *) a;
  const int *i_b = (const int *) b;
  
  return *i_a - *i_b;
}

void insertSort(char *first, char *last, size_t size, FP_COMPARE compare) {

  char *high_key = (char *)first + size;

  while(high_key <= last) {

    char *insert_key = high_key;
    char *scan_key = insert_key - size;
    
    //scan for first key that is smaller or equal than key to be inserted
    while(scan_key >= first && compare((void *) insert_key, (void *) scan_key) < 0) {
      swap(insert_key, scan_key, size);
      scan_key -= size;
      insert_key -= size;
    }

    high_key += size;
  }


}


/* Moves middle element to midposition and makes it pivot */
char *selectPivot(char **p_left, char **p_right, size_t size, FP_COMPARE compare) {

  //assert(length >= 3);

  char *mid = *p_left + ((*p_right - *p_left) / (2*size)) * size;

  //mid smaller than first?
  if(compare((void *) mid, (void *) *p_left) < 0) {
    swap(mid, *p_left, size);
  }
  //mid now bigger than last?
  if(compare((void *) mid, (void *) *p_right) > 0) {
    swap(mid, *p_right, size);
    //was end smaller than first?
    if(compare((void *) mid, (void *) *p_left) < 0) {
      swap(mid, *p_left, size);
    }
  }

  *p_left += size;
  *p_right -= size;

  return mid;
}



void partition(char **p_left, char **p_right, char *pivot, size_t size, FP_COMPARE compare) {

  char *left = *p_left;
  char *right = *p_right;

  do {
    while(compare((void *) left, (void *) pivot) < 0) {
      left += size;
    }
    while(compare((void *) pivot, (void *) right) < 0) {
      right -= size;
    }

    if(left < right) {
      swap(left, right, size);
      //did we swap the pivot?
      if(left == pivot) {
        pivot = right; 
      }
      else if(right == pivot) {
        pivot = left;
      }
      left += size;
      right -= size;
    }
    else if(left == right) {
      //met on same value == pivot
      left += size;
      right -= size;
      break;
    }
  } while(left <= right);

  *p_left = left;
  *p_right = right;
}


void qs(char *start, char *end, size_t size, FP_COMPARE compare) {
  if(end-start+size > CUTOFF*size) {

    char *left = start;
    char *right = end;

    char *pivot = selectPivot(&left, &right, size, compare);

    partition(&left, &right, pivot, size, compare);

    size_t d_low = right - start;
    size_t d_high = end - left;
    if(d_low < d_high) {
      //do low half first
      qs(start, right, size, compare);
      qs(left, end, size, compare);
    }
    else {
      //high half first
      qs(left, end, size, compare);
      qs(start, right, size, compare);
    }

  }
  else {
    insertSort(start, end, size, compare);
  }
}

void qs_stack(char *start, char *end, size_t size, FP_COMPARE compare) {

  size_t max_depth = sizeof(size_t)*2;
  char *stackspace[max_depth*sizeof(char *)];
  PSTACK stack;
  PSTACK *pstack = &stack;
  pstack_init(pstack, stackspace, sizeof(stackspace));

  do {

    if(end-start+size > CUTOFF*size) {

      char *left = start;
      char *right = end;

      char *pivot = selectPivot(&left, &right, size, compare);

      partition(&left, &right, pivot, size, compare);

      size_t d_low = right - start;
      size_t d_high = end - left;
      if(d_low < d_high) {
        //do low half first
        pstack_push(left, pstack);    //start for high half
        pstack_push(end, pstack);     //end for high half
        end = right;
      }
      else {
        //high half first
        pstack_push(start, pstack);   //start for low half
        pstack_push(right, pstack);   //end for low half
        start = left;
      }

    }
    else {
      insertSort(start, end, size, compare);
      if(pstack_empty(pstack)) {
        break;
      }
      end = pstack_pop(pstack);
      start = pstack_pop(pstack);
    }
  } while(TRUE);
}

typedef struct
  {
    char *lo;
    char *hi;
  } stack_entry;

/* from libc qsort... */

#define PUSH(start, end) ((void) ((sp->lo = (start)), (sp->hi = (end)), ++sp))
#define	POP(start, end) ((void) (--sp, (start = sp->lo), (end = sp->hi)))
#define	STACK_EMPTY (sp <= stack)

void qs_stack2(char *start, char *end, size_t size, FP_COMPARE compare) {

  size_t max_depth = sizeof(size_t);    //actually log(n)...
  stack_entry stack[max_depth*sizeof(stack_entry)];
  stack_entry *sp = &stack[0];

  do {

    if(end-start+size > CUTOFF*size) {

      char *left = start;
      char *right = end;

      char *pivot = selectPivot(&left, &right, size, compare);

      partition(&left, &right, pivot, size, compare);

      size_t d_low = right - start;
      size_t d_high = end - left;
      if(d_low < d_high) {
        //do low half first
        PUSH(left, end);             //start & end for high half
        end = right;
      }
      else {
        //high half first
        PUSH(start, right);           //start & end for low half
        start = left;
      }

    }
    else {
      insertSort(start, end, size, compare);
      if(STACK_EMPTY) {
        break;
      }
      POP(start, end);
    }
  } while(TRUE);
}


void quicksort(void *items, size_t length, size_t size, FP_COMPARE compare) {
  if(items != NULL) {
    qs((char *) items, (char *) items + (length*size - size), size, compare);
  }
}

void quicksort_stack(void *items, size_t length, size_t size, FP_COMPARE compare) {
  if(items != NULL) {
    qs_stack2((char *) items, (char *) items + (length*size - size), size, compare);
  }
}


void print_ints(int items[], int len) {
  if(len <= 100 && len > 0) {
    int i;
    for(i=0; (i+1)<len; i++) {
      printf("%d, ",items[i]);
    }
    printf("%d\n",items[i]);
  }
}

int same(void *_a, void *_b, size_t len) {
  char *a = (char *) _a;
  char *b = (char *) _b;

  while(len > 0 && *a++ == *b++) {
    len--;
  }

  return len == 0;
}

double get_millis() {
  struct timeval now;
  gettimeofday(&now, NULL);  //error checking?
  double t = now.tv_usec/1000.0;
  t += now.tv_sec*1000;
  return t;
}

void print_usage(void) {
  printf(" -n SIZE -s SEED -d DISTRIBUTION -t NUM_THREADS\n");
}

int get_options(int argc, char *argv[], struct _opts *options) {

  int success = TRUE;
  int c;


  while ((c = getopt(argc, argv, "n:s:t:p:")) != -1) {
    switch (c) {
      case 'n':
        options->size = (uint32_t) strtol(optarg, NULL, 0);
        break;
      case 's':
        options->seed = (uint32_t) strtol(optarg, NULL, 0);
        break;
      case 't':
        options->threads = (uint32_t) strtol(optarg, NULL, 0);
        break;
      case 'd':
        options->pattern = (uint32_t) strtol(optarg, NULL, 0);
        break;
      case '?':
        success = FALSE;
        if (optopt == 'c') {
          printf("Option -%c requires an argument.\n", optopt);
        }
        else {
          printf("Unknown option '-%c'.\n", optopt);
        }
        print_usage();
        break;
      default:
        break;
    }
  }

  return success;

}

#define ITEMS 20000

int main(int argc, char *argv[]) {

  struct _opts prog_options;

  prog_options.threads = 0;
  prog_options.seed = 0;
  prog_options.pattern = 0;
  prog_options.size = ITEMS;

  if(!get_options(argc, argv, &prog_options)) {
    return 1;
  }

  size_t n = prog_options.size;
  





  int *a = (int *) generate_work(n, sizeof(int), generate_item, prog_options.seed, 0);
  int *b = (int *) malloc(n*sizeof(int));
  memcpy(b, a, n*sizeof(int));
  
  print_ints(a, n);
  
  double start = get_millis();
//  insertSort(a, &a[n-1], sizeof(int), compare_ints);
  quicksort_stack((void *) a, n, sizeof(int), compare_ints);
  double stop = get_millis();
  
//  printf("Insertion sort took %.3f ms\n", stop-start);
  printf("Quicksort took %.3f ms\n", stop-start);

/*
  int *left, *right, *pivot;
  left = &a[0];
  right = &a[n-1];
  pivot = (int *) selectPivot((char **) &left, (char **) &right, sizeof(int), compare_ints);

  printf("f: %d l: %d p: %d r: %d e: %d\n", a[0], *left, *pivot, *right, a[n-1]);  
*/
  print_ints(a, n);



  start = get_millis();
  qsort(b, n, sizeof(int), compare_ints);
  stop = get_millis();
  printf("Libc qsort took %.3f ms\n", stop-start);

  if(!same(a, b, sizeof(int)*n)) {
    printf("Sorting error!\n");
  }


  free(a);
  free(b);

  return 0;
}
