#ifndef RUN_H
#define RUN_H

#include <vector>

#include <string.h>
#include "types.h"
#include "bloom_filter.h"

#define TMP_FILE_PATTERN "/tmp/lsm-XXXXXX"

using namespace std;

class Run {
    BloomFilter bloom_filter;
    vector<KEY_t> fence_pointers;
    KEY_t max_key;
    KEY_t min_key;
    entry_t *mapping;
    size_t mapping_length;
    int mapping_fd;
    long file_size() {return max_size * sizeof(entry_t);}
public:
    long size, max_size;
    string tmp_file;
    Run(long, float);
    ~Run(void);
    entry_t * map_read(size_t, off_t);
    entry_t * map_read(void);
    entry_t * map_write(void);
    void unmap(void);
    VAL_t * get(KEY_t);
    vector<entry_t> * range(KEY_t, KEY_t);
    void put(entry_t);
    bool remaining(void) const 
    {
        if (max_size - size > 0)
            return 1;
        else
            return 0;
    }
};
#endif
