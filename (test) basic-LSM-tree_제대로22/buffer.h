/*
	실험용 basic LSM 코드 수정본
*/
#include <set>
#include <vector>
#include <tuple>
#include "types.h"
#include "run.h"
#include "B+ Tree.h"
using namespace std;

class Buffer {
public:
    int max_size;
    set<entry_t> entries;
    Buffer(int max_size) : max_size(max_size) {};
    VAL_t * get(KEY_t) const;
    vector<entry_t> * range(KEY_t, KEY_t) const;
    bool put(KEY_t, VAL_t val);
	//bool put(KEY_t, VAL_t val, BPTree**);
    void empty(void);
    
	void insertionMethod(BPTree**, KEY_t);

};
