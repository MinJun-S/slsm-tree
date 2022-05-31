/*
	실험용 basic LSM 코드 수정본
*/
#ifndef RUN_H
#define RUN_H

#include <vector>
#include <set>

#include <string.h>
#include "types.h"
#include "bloom_filter.h"
#include "rtree.h"

//#define TMP_FILE_PATTERN "/tmp/lsm-XXXXXX"

using namespace std;

class Run {
    BloomFilter bloom_filter;
    vector<KEY_t> fence_pointers;  
    size_t mapping_length;
    int mapping_fd;
    long file_size() { return max_size * sizeof(entry_t) * 2; }
public:
    RTREENODE* root;
    set<entry_t> entries;
    int idx_level, idx_run;
    long size, max_size;
    string tmp_file;
    KEY_t max_key;
    Run(long, float, int);
    ~Run(void);
    VAL_t * get(KEY_t);
    vector<entry_t> * range(KEY_t, KEY_t);
    void put(entry_t);
    void empty();
    bool remaining(void) const 
    {
        if (max_size - size > 0)
            return 1;
        else
            return 0;
    }
    string run_name;
};
#endif
