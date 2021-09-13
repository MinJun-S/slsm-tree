#include <vector>

#include "buffer.h"
#include "level.h"
#include "spin_lock.h"
#include "types.h"
#include "worker_pool.h"

#define DEFAULT_TREE_DEPTH 5
#define DEFAULT_TREE_FANOUT 10
#define DEFAULT_BUFFER_NUM_PAGES 1000
#define DEFAULT_THREAD_COUNT 4
#define DEFAULT_BF_BITS_PER_ENTRY 0.5

#define GEO_X_MIN  125.06666667
#define GEO_X_MAX  131.87222222
#define GEO_Y_MIN  33.10000000
#define GEO_Y_MAX  38.45000000

class LSMTree {
    Buffer buffer;
    WorkerPool worker_pool;
    float bf_bits_per_entry;
    vector<Level> levels;
    Run * get_run(int);
    void merge_down(vector<Level>::iterator);
public:
    LSMTree(int, int, int, int, float);
    void put(KEY_t, VAL_t);
    void get(KEY_t);
    void range(KEY_t, KEY_t);
    void del(KEY_t);
    void load(std::string);
};

KEY_t make_key(float x, float y);