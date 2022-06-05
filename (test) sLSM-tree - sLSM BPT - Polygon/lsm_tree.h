/*
	실험용 basic LSM 코드 수정본
*/
#include <vector>
#include <set>
#include <algorithm>
#include <utility>
#include <iomanip>
#include <cmath>

#include "buffer.h"
#include "level.h"
#include "spin_lock.h"
#include "types.h"
#include "worker_pool.h"
#include "B+ Tree.h"

using namespace std;

#define DEFAULT_TREE_DEPTH 10
#define DEFAULT_TREE_FANOUT 1
#define DEFAULT_BUFFER_NUM_PAGES 1000
#define DEFAULT_THREAD_COUNT 4
#define DEFAULT_BF_BITS_PER_ENTRY 0.5

#define GEO_X_MIN  125.06666667
#define GEO_X_MAX  131.87222222
#define GEO_Y_MIN  33.10000000
#define GEO_Y_MAX  38.45000000

#define LIMIT_LEVEL 3  // 4분할하는 레벨 조정

class LSMTree {
    Buffer buffer;
    WorkerPool worker_pool;
    float bf_bits_per_entry;
    vector<Level> levels;
    Run * get_run(int);
    void merge_down(vector<Level>::iterator, int, FILE* filePtr);
public:
    set<int> Q_filter;
    LSMTree(int, int, int, int, float);
	void put(KEY_t, VAL_t, FILE* filePtr, MBR_LB_t MBR_LB, MBR_UB_t MBR_UB, POLY_t poly_name);
	//void put(KEY_t, VAL_t, BPTree**);
    void get(KEY_t);
    void range(KEY_t, KEY_t);
    void del(KEY_t);
    void load(std::string);
    void print_tree();
    void save_run();
    void save_file();
    set<int> Create_Query_filter(VAL_t, float);           // range query
    void reset_Q_filter();
    float Compute_Overlap(VAL_t, VAL_t, VAL_t, VAL_t);

    void range_query(entry_t, float);
    float Compute_distance(entry_t, entry_t);

    void KNN_query1(entry_t, int);
    void KNN_query2(entry_t, int);
    int KNN_query1_bptree(entry_t, int);
    int KNN_query2_bptree(entry_t, int);
    //set<pair<float, entry_t>> NN_range(int, int, entry_t, float);
    
	int IO_Check;
    
	//void searchMethod(BPTree*, KEY_t, KEY_t);
	void searchMethod(entry_t, float);
};

KEY_t make_key(float x, float y);