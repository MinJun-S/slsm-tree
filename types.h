#ifndef TYPES_H
#define TYPES_H
#include <stdint.h>

typedef uint32_t KEY_t;
struct VAL_t  //struct로 변경
{
    float x;
    float y; 
};

#define KEY_MAX 4294967295  
#define KEY_MIN 0

#define VAL_MAX 2147483647
#define VAL_MIN -2147483647
#define VAL_TOMBSTONE -2147483648

#define VAL_MAX_X 131.87222222 
#define VAL_MIN_X 125.06666667
#define VAL_MAX_Y 38.45000000
#define VAL_MIN_Y 33.10000000

struct entry {
    KEY_t key;
    VAL_t val;
    bool operator==(const entry& other) const {return key == other.key;}
    bool operator<(const entry& other) const {return key < other.key;}   //나중에 중복 key도 모두 출력할 수 있도록 수정 
    bool operator>(const entry& other) const {return key > other.key;}
};

typedef struct entry entry_t;

#endif
