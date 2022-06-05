/*
	����� basic LSM �ڵ� ������
*/
#ifndef TYPES_H
#define TYPES_H
#include <stdint.h>

typedef uint32_t KEY_t;
typedef int POLY_t;
struct VAL_t  //struct�� ����
{
    float x;
    float y; 
};
struct MBR_LB_t  //struct�� ����
{
	float x;
	float y;
};
struct MBR_UB_t  //struct�� ����
{
	float x;
	float y;
};

#define KEY_MAX 4294967295
#define KEY_MIN 0

#define VAL_MAX 2147483647
#define VAL_MIN -2147483647
#define VAL_TOMBSTONE -2147483648

/*
* // Original (wooyeol code)
#define VAL_MAX_X 131.87222222 
#define VAL_MIN_X 125.06666667
#define VAL_MAX_Y 38.45000000
#define VAL_MIN_Y 33.10000000
*/

#define VAL_MAX_X 131.87222221
#define VAL_MIN_X 125.06666668
#define VAL_MAX_Y 38.44000000
#define VAL_MIN_Y 33.11000000

struct entry {
    KEY_t key;
    VAL_t val;
	MBR_LB_t MBR_LB;   // Lower bound of Polygon (�ϳ��� MBR �� ��� Point�� �����ϰ�)  -> ������Ʈ ���̺��� ���ԵǾ��ִٰ� ����
	MBR_UB_t MBR_UB;   // Upper bound of Polygon (�ϳ��� MBR �� ��� Point�� �����ϰ�)  -> ������Ʈ ���̺��� ���ԵǾ��ִٰ� ����
	POLY_t poly_name;     // Polygon name (�ϳ��� MBR �� ��� Point�� �����ϰ�)            -> ������Ʈ ���̺��� ���ԵǾ��ִٰ� ����
    bool operator==(const entry& other) const {return key == other.key;}
    bool operator<(const entry& other) const {return key < other.key;}
    bool operator>(const entry& other) const {return key > other.key;}
};

typedef struct entry entry_t;

#endif
