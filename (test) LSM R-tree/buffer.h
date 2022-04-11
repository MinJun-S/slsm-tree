/*
	실험용 basic LSM 코드 수정본
*/
#include <set>
#include <vector>
#include <tuple>
#include "types.h"
#include "run.h"

using namespace std;

class Buffer {
public:
    int max_size;
    set<entry_t> entries;
    Buffer(int s) {
        max_size = 4096;
        buffer_size = 0;
        RTREENODE* root = RTreeCreate();
    };
    VAL_t * get(KEY_t) const;
    vector<entry_t> * range(KEY_t, KEY_t) const;
    bool put(KEY_t, VAL_t val);
    void empty(void);
    RTREENODE* root;
    int buffer_size;
};
