#include <cassert>
#include <fstream>
#include <iostream>
#include <map>
#include <cmath>
#include <string>

#include "lsm_tree.h"
#include "merge.h"
#include "sys.h"

using namespace std;

ostream& operator<<(ostream& stream, const entry_t& entry) {
    stream.write((char *)&entry.key, sizeof(KEY_t));
    stream.write((char *)&entry.val, sizeof(VAL_t));
    return stream;
}

istream& operator>>(istream& stream, entry_t& entry) {
    stream.read((char *)&entry.key, sizeof(KEY_t));
    stream.read((char *)&entry.val, sizeof(VAL_t));
    return stream;
}

/*
 * LSM Tree
 */

LSMTree::LSMTree(int buffer_max_entries, int depth, int fanout,
                 int num_threads, float bf_bits_per_entry) :
                 bf_bits_per_entry(bf_bits_per_entry),
                 buffer(buffer_max_entries),
                 worker_pool(num_threads)
{
    vector<Level>::iterator current;
    KEY_t max_key;
    KEY_t min_key;
    long max_run_size;
    int depth_run = depth;
    max_run_size = buffer_max_entries;

    while ((depth--) > 0) {
        if (fanout < 64) {
            fanout *= 4;
        }
        else {
            max_run_size *= 2;
        }
        levels.emplace_back(fanout, max_run_size);
    }

    current = levels.begin();
    int cnt = 0;
    while ((depth_run--) > 0) {
        cnt += 1;
        if (cnt == 1) {
            int temp = KEY_MAX / 4;
            for (int i = 0; i < 4; i++) {
                min_key = temp * i;
                max_key = temp * (i + 1) - 1; 
                Run* tmp1 = new Run(current->max_run_size, bf_bits_per_entry, max_key, min_key, 1, i);                
                current->runs_list[i] = tmp1;
                current->runs_list[i]->spatial_filter[0] = 0; current->runs_list[i]->spatial_filter[1] = 0; current->runs_list[i]->spatial_filter[2] = 0; current->runs_list[i]->spatial_filter[3] = 0;
            }
        }
        else if (cnt == 2) {
            int temp = KEY_MAX / 16;
            for (int i = 0; i < 16; i++) {
                min_key = temp * i;
                max_key = temp * (i + 1) - 1;
                Run* tmp1 = new Run(current->max_run_size, bf_bits_per_entry, max_key, min_key, 2, i);
                current->runs_list[i] = tmp1;
                current->runs_list[i]->spatial_filter[0] = 0; current->runs_list[i]->spatial_filter[1] = 0; current->runs_list[i]->spatial_filter[2] = 0; current->runs_list[i]->spatial_filter[3] = 0;
            }
        }
        else {
            int temp = KEY_MAX / 64;
            for (int i = 0; i < 64; i++) {
                min_key = temp * i;
                max_key = temp * (i + 1) - 1;
                Run* tmp1 = new Run(current->max_run_size, bf_bits_per_entry, max_key, min_key, cnt, i);
                current->runs_list[i] = tmp1;
                current->runs_list[i]->spatial_filter[0] = 0; current->runs_list[i]->spatial_filter[1] = 0; current->runs_list[i]->spatial_filter[2] = 0; current->runs_list[i]->spatial_filter[3] = 0;
            }
        }
        cout << cnt << endl;
        current += 1;
    }
}

void LSMTree::merge_down(vector<Level>::iterator current, int idx) {
    vector<Level>::iterator next;
    Run** runs_list;
    MergeContext merge_ctx;
    entry_t entry;

    KEY_t temp;

    KEY_t max_key;
    KEY_t min_key;
  
    assert(current >= levels.begin());
    cout << "step0_assert" << endl;

    if (current->runs_list[idx]->remaining() > 0) {
        return;
    }
    else if (current >= levels.end() - 1) {
        die("no more space in tree.");
    }
    else {
        cout << "\n ┏------------- [ START : Merge Down ] -------------┓\n" << endl;
        next = current + 1;
    }

    /*
     * Merge all runs in the current level into the first
     * run in the next level
     */

    int level_temp = current->runs_list[idx]->idx_level + 1;

    string st_file_name;
    char ch_file_name[100];
    string input_data;
    char ch_input_data[100];
    int i = idx * 4;
    FILE* fp = NULL;
    temp = KEY_MAX / pow(4, current->runs_list[idx]->idx_level);
    if (current->runs_list[idx]->idx_level < 3) {
        //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // generate file
        st_file_name = "runs_list/" + to_string(level_temp) + "_" + to_string(i) + ".txt";
        cout << "\n * " << st_file_name << endl;
        strcpy(ch_file_name, st_file_name.c_str());
        fp = fopen(ch_file_name, "w"); //test파일을 w(쓰기) 모드로 열기
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 
        max_key = next->runs_list[i]->max_key;
        for (const auto& entry : current->runs_list[idx]->entries) 
        {
            if (entry.key > max_key)
            {
                fclose(fp);       //파일 포인터 닫기//////////////////////////////////////////////////////////////////////<------------this 

                i++;
                max_key = next->runs_list[i]->max_key;

                //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                // generate file
                st_file_name = "runs_list/" + to_string(level_temp) + "_" + to_string(i) + ".txt";
                cout << "\n * " << st_file_name << endl;
                strcpy(ch_file_name, st_file_name.c_str());
                fp = fopen(ch_file_name, "w"); //test파일을 w(쓰기) 모드로 열기
                ////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 
            }

            /*
            entry_t tmp;
            tmp.key = entry.key; tmp.val = entry.val;
            levels.front().runs_list[i]->put(tmp);
            */
            next->runs_list[i]->put(entry);
            next->runs_list[i]->spatial_filter[(int)((entry.key - next->runs_list[i]->min_key) / (temp / 4))] = 1;
            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // input to txt file
            input_data = to_string(entry.val.x) + "  " + to_string(entry.val.y) + "  " + to_string(entry.key) + "\n";
            strcpy(ch_input_data, input_data.c_str());
            fputs(ch_input_data, fp); //문자열 입력
            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        }
        fclose(fp);       //파일 포인터 닫기//////////////////////////////////////////////////////////////////////<------------this 
    }
    else {
        //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // generate file
        st_file_name = "runs_list/" + to_string(level_temp) + "_" + to_string(idx) + ".txt";
        cout << "\n * " << st_file_name << endl;
        strcpy(ch_file_name, st_file_name.c_str());
        fp = fopen(ch_file_name, "w"); //test파일을 w(쓰기) 모드로 열기
        //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        for (const auto& entry : current->runs_list[idx]->entries) {
            next->runs_list[idx]->put(entry);
            next->runs_list[idx]->spatial_filter[(int)((entry.key - next->runs_list[idx]->min_key) / (temp / 4))] = 1;
            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // input to txt file
            input_data = to_string(entry.val.x) + "  " + to_string(entry.val.y) + "  " + to_string(entry.key) + "\n";
            strcpy(ch_input_data, input_data.c_str());
            fputs(ch_input_data, fp); //문자열 입력
            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        }
        fclose(fp);       //파일 포인터 닫기//////////////////////////////////////////////////////////////////////<------------this 
    }

    /*
     * if the next level does not have space for the current level,
     * recursively merge the next level downwards to create some
     */

    if (current->runs_list[idx]->idx_level < 3) {
        for (int i = 4 * idx; i < 4 * idx + 4; i++) {
            if (next->runs_list[i]->remaining() <= 0) {
                merge_down(next, i);
                assert(next->runs_list[i]->remaining() > 0);
            }
        }
    }
    else {
        if (next->runs_list[idx]->remaining() <= 0) {
            merge_down(next, idx);
            assert(next->runs_list[idx]->remaining() > 0);
        }
    }

    /*
     * Clear the current level to delete the old (now redundant) entry files.
     */
    KEY_t min_temp = current->runs_list[idx]->min_key;
    KEY_t max_temp = current->runs_list[idx]->max_key;
    Run* tmp = new Run(current->max_run_size, bf_bits_per_entry, max_temp, min_temp, level_temp - 1, idx);

    current->runs_list[idx]->empty();
    //delete current->runs_list[idx];

    
    current->runs_list[idx] = tmp;
    current->runs_list[idx]->spatial_filter[0] = 0; current->runs_list[idx]->spatial_filter[1] = 0; current->runs_list[idx]->spatial_filter[2] = 0; current->runs_list[idx]->spatial_filter[3] = 0;
    cout << "\n ┗------------- [ END : Merge Down ] -------------┛\n" << endl;
}

void LSMTree::put(KEY_t key, VAL_t val) {
    KEY_t max_key;
    KEY_t min_key;
    /*
     * Try inserting the key into the buffer
     */
    if (buffer.put(key, val)) {
        return;
    }

    /*
     * If the buffer is full, flush level 0 if necessary to create space
     */

    for (int i = 0; i < 4; i++) {
        if (levels.front().runs_list[i] != NULL) {
            merge_down(levels.begin(), i);
        }
    }

    /*
     * Flush the buffer to level 0
     */
    int i = 0; KEY_t temp = KEY_MAX;                  // 'i' is a run index of levels
    min_key = 0;
    max_key = temp / 4 - 1;
    int loop = 0;

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // generate file    
    string st_file_name;        
    char ch_file_name[100];
    st_file_name = "runs_list/" + to_string(levels.front().runs_list[i]->idx_level) + "_" + to_string(i) + ".txt";
    cout << "\n * " << st_file_name << endl;
    strcpy(ch_file_name, st_file_name.c_str());
    FILE* fp = NULL;
    fp = fopen(ch_file_name, "w"); //test파일을 w(쓰기) 모드로 열기
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    string input_data;
    char ch_input_data[100];

    for (const auto& entry : buffer.entries) 
    {
        if (entry.key > max_key)
        {
            fclose(fp);       //파일 포인터 닫기//////////////////////////////////////////////////////////////////////<------------this 

            i++;
            min_key = (temp / 4) * i;
            max_key = (temp/ 4) * (i + 1) - 1;
            // if(i==3) max_key++;

            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // generate file            
            st_file_name = "runs_list/" + to_string(levels.front().runs_list[i]->idx_level) + "_" + to_string(i) + ".txt";
            cout << "\n * " << st_file_name << endl;
            strcpy(ch_file_name, st_file_name.c_str());
            fp = fopen(ch_file_name, "w"); //test파일을 w(쓰기) 모드로 열기            
            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        }

        /*
        entry_t tmp;
        tmp.key = entry.key; tmp.val = entry.val;
        levels.front().runs_list[i]->put(tmp);
        */
        levels.front().runs_list[i]->put(entry);
        levels.front().runs_list[i]->spatial_filter[(int)((entry.key - levels.front().runs_list[i]->min_key) / (temp / 16))] = 1;

        //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // input to txt file                
        input_data = to_string(entry.val.x) + "  " + to_string(entry.val.y) + "  " + to_string(entry.key) + "\n";
        strcpy(ch_input_data, input_data.c_str());
        fputs(ch_input_data, fp); //문자열 입력        
        //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    }

    fclose(fp);      //파일 포인터 닫기/////////////////////////////////////////////////////<-------얘도


    /*    *   Save Buffer's entries    */    
    st_file_name = "runs_list/0_Buffer.txt";
    cout << "\n * " << st_file_name << endl;
    strcpy(ch_file_name, st_file_name.c_str());
    fp = fopen(ch_file_name, "w"); //test파일을 w(쓰기) 모드로 열기
    for (const auto& entry : buffer.entries) {
        // input to txt file
        input_data = to_string(entry.val.x) + "  " + to_string(entry.val.y) + "  " + to_string(entry.key) + "\n";
        strcpy(ch_input_data, input_data.c_str());
        fputs(ch_input_data, fp); //문자열 입력
    }
    fclose(fp);       //파일 포인터 닫기//////////////////////////////////////////////////////////////////////<------------this

    buffer.empty();
    cout << buffer.entries.size() << endl;

    assert(buffer.put(key, val));
}

Run * LSMTree::get_run(int index) {
    for (const auto& level : levels) {
        if (index < level.runs.size()) {
            return (Run *) &level.runs[index];
        } else {
            index -= level.runs.size();
        }
    }

    return nullptr;
};

void LSMTree::get(KEY_t key) {
    VAL_t *buffer_val;
    VAL_t latest_val;
    int latest_run;
    SpinLock lock;
    atomic<int> counter;

    /*
     * Search buffer
     */

    buffer_val = buffer.get(key);

    if (buffer_val != nullptr) {
        if (buffer_val->x != VAL_TOMBSTONE || buffer_val->y != VAL_TOMBSTONE) cout << buffer_val->x<<", "<<buffer_val->y; //val °ª ¼öÁ¤
        cout << endl;
        delete buffer_val;
        return;
    }

    /*
     * Search runs
     */

    counter = 0;
    latest_run = -1;

    worker_task search = [&] {
        int current_run;
        Run *run;
        VAL_t *current_val;

        current_run = counter++;

        if (latest_run >= 0 || (run = get_run(current_run)) == nullptr) {
            // Stop search if we discovered a key in another run, or
            // if there are no more runs to search
            return;
        } else if ((current_val = run->get(key)) == nullptr) {
            // Couldn't find the key in the current run, so we need
            // to keep searching.
            search();
        } else {
            // Update val if the run is more recent than the
            // last, then stop searching since there's no need
            // to search later runs.
            lock.lock();

            if (latest_run < 0 || current_run < latest_run) {
                latest_run = current_run;
                latest_val = *current_val;
            }

            lock.unlock();
            delete current_val;
        }
    };

    worker_pool.launch(search);
    worker_pool.wait_all();

    if (latest_run >= 0 && latest_val.x != VAL_TOMBSTONE && latest_val.y != VAL_TOMBSTONE) cout << latest_val.x <<", "<<latest_val.y;
    cout << endl;
}

void LSMTree::range(KEY_t start, KEY_t end) {
    vector<entry_t> *buffer_range;
    map<int, vector<entry_t> *> ranges;
    SpinLock lock;
    atomic<int> counter;
    MergeContext merge_ctx;
    entry_t entry;

    if (end <= start) {
        cout << endl;
        return;
    } else {
        // Convert to inclusive bound
        end -= 1;
    }

    /*
     * Search buffer
     */

    ranges.insert({0, buffer.range(start, end)});

    /*
     * Search runs
     */

    counter = 0;

    worker_task search = [&] {
        int current_run;
        Run *run;

        current_run = counter++;

        if ((run = get_run(current_run)) != nullptr) {
            lock.lock();
            ranges.insert({current_run + 1, run->range(start, end)});
            lock.unlock();

            // Potentially more runs to search.
            search();
        }
    };

    worker_pool.launch(search);
    worker_pool.wait_all();

    /*
     * Merge ranges and print keys
     */

    for (const auto& kv : ranges) {
        merge_ctx.add(kv.second->data(), kv.second->size());
    }

    while (!merge_ctx.done()) {
        entry = merge_ctx.next();

        if (entry.val.x != VAL_TOMBSTONE && entry.val.y != VAL_TOMBSTONE) {
            cout << entry.key << ":" << entry.val.x<<", "<<entry.val.y;
            if (!merge_ctx.done()) cout << " ";
        }
    }

    cout << endl;

    /*
     * Cleanup subrange vectors
     */

    for (auto& range : ranges) {
        delete range.second;
    }
}

void LSMTree::del(KEY_t key) {
    VAL_t temp;
    temp.x = VAL_TOMBSTONE; temp.y = VAL_TOMBSTONE;
    put(key, temp);
}

void LSMTree::load(string file_path) {
    ifstream stream;
    entry_t entry;

    stream.open(file_path, ifstream::binary);

    if (stream.is_open()) {
        while (stream >> entry) {
            put(entry.key, entry.val);
        }

    } else {
        die("Could not locate file '" + file_path + "'.");
    }
}

void show(KEY_t key)
{
    for (int i = 32; i >= 0; i--)
    {
        printf("%d", (key >> i) & 1);
    }
    printf("  %u \n",key);
}

KEY_t make_key(float x, float y)
{
    float x1 = GEO_X_MIN; float x2 = GEO_X_MAX; float y1 = GEO_Y_MIN; float y2 = GEO_Y_MAX;
    float x_md, y_md;
    KEY_t key=0;
    KEY_t temp = 1; 
    temp = temp << 31;
    //show(temp);
    for (int i = 63; i >= 0; i--) //15
    {
        x_md = (x2 + x1) / 2;
        y_md = (y2 + y1) / 2;
        if (x > x_md)
        {
            key += temp;
            x1 = x_md;
        }
        else x2 = x_md;
        temp = temp >> 1;
        //show(temp);
        if (y > y_md)
        {
            key += temp;
            y1 = y_md;
        }
        else y2 = y_md;
        temp = temp >> 1;
        //show(temp);
        //printf("%d %d %d \n", i, temp, key);
    }
    //show(key);

    return key;
}

void LSMTree::print_tree()
{
    int i = 0;
    for (const auto& entry : buffer.entries)
    {
        cout << entry.val.x << " " << entry.val.y << " " << entry.key <<  "\n";
        i++;
    }
    cout << "number of entry in Buffer = " << i << endl;

}


void LSMTree::save_run()
{
    cout << "start save" << endl;

    vector<Level>::iterator current;
    MergeContext merge_ctx;
    entry_t entry;
    string st_file_name;
    char ch_file_name[100];
    string input_data;
    char ch_input_data[100];

    current = levels.begin();
    int idx = 0;
    FILE* fp = NULL;
    for (int j = 1; j <= levels.size(); j++) {
        cout << j << endl;
        if (current->runs_list[idx]->idx_level <= 3 && current->runs_list[idx] != NULL) {
            for (int i = 0; i < int(pow(4, j)); i++) {
                if (current->runs_list[i] != NULL) {
                    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                    // generate file
                    st_file_name = "run_list/" + to_string(j) + "_" + to_string(i) + ".txt";
                    cout << st_file_name << endl;
                    strcpy(ch_file_name, st_file_name.c_str());
                    fp = fopen(ch_file_name, "w"); //test파일을 w(쓰기) 모드로 열기
                    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////     
                    for (const auto& entry : current->runs_list[idx]->entries)
                    {
                        //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                        // input to txt file
                        input_data = to_string(entry.val.x) + "  " + to_string(entry.val.y) + "  " + to_string(entry.key) + "\n";
                        strcpy(ch_input_data, input_data.c_str());
                        fputs(ch_input_data, fp); //문자열 입력
                        //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                    }
                    fclose(fp);       //파일 포인터 닫기//////////////////////////////////////////////////////////////////////<------------this 
                }
                else break;
            }
        }
        else if (current->runs_list[idx]->idx_level > 3 && current->runs_list[idx] != NULL) {
            for (int i = 0; i < 64; i++) {
                if (current->runs_list[i] != NULL) {
                    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                    // generate file
                    st_file_name = "run_list/" + to_string(j) + "_" + to_string(i) + ".txt";
                    cout << st_file_name << endl;
                    strcpy(ch_file_name, st_file_name.c_str());
                    fp = fopen(ch_file_name, "w"); //test파일을 w(쓰기) 모드로 열기
                    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////     
                    for (const auto& entry : current->runs_list[idx]->entries)
                    {
                        //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                        // input to txt file
                        input_data = to_string(entry.val.x) + "  " + to_string(entry.val.y) + "  " + to_string(entry.key) + "\n";
                        strcpy(ch_input_data, input_data.c_str());
                        fputs(ch_input_data, fp); //문자열 입력
                        //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                    }
                    fclose(fp);       //파일 포인터 닫기//////////////////////////////////////////////////////////////////////<------------this 
                }
                else break;
            }
        }
        else break;
        current += 1;
    }
}


set<int> LSMTree::Create_Query_filter(VAL_t query_val, float range) 
{
    VAL_t lb; VAL_t ub;                 // lower and upper of query value's range
    VAL_t min_key; VAL_t max_key;       // lower and upper of each MBR

    //set<int> Q_filter;                // -> Change Global value

    // Compute lower and upper from query_val
    lb.x = query_val.x - range; lb.y = query_val.y - range;
    ub.x = query_val.x + range; ub.y = query_val.y + range;

    float measure = 0;
    float x_size; 
    float y_size;    

    struct coordinate
    {
        int x;
        int y;
    };

    struct coordinate zindex[64] = {
        {0,0},{0,1},{1,0},{1,1},
        {0,2},{0,3},{1,2},{1,3},
        {2,0},{2,1},{3,0},{3,1},
        {2,2},{2,3},{3,2},{3,3},

        {0,4},{0,5},{1,4},{1,5},
        {0,6},{0,7},{1,6},{1,7},
        {2,4},{2,5},{3,4},{3,5},
        {2,6},{2,7},{3,6},{3,7},  ////// 32 z-order num(x,y) in left

        {4,0},{4,1},{5,0},{5,1},
        {4,2},{4,3},{5,2},{5,3},
        {6,0},{6,1},{7,0},{7,1},
        {6,2},{6,3},{7,2},{7,3},

        {4,4},{4,5},{5,4},{5,5},
        {4,6},{4,7},{5,6},{5,7},
        {6,4},{6,5},{7,4},{7,5},
        {6,6},{6,7},{7,6},{7,7}  ////// 32 z-order num(x,y) in right
    };

    // Compute lower and upper of each MBR and Overlap Check with range of query_value
    x_size = (GEO_X_MAX - GEO_X_MIN) / 8;
    y_size = (GEO_Y_MAX - GEO_Y_MIN) / 8;

    for (int i = 0; i < 64; i++) {     // 64
        min_key.x = (x_size * zindex[i].x) + GEO_X_MIN;
        min_key.y = (y_size * zindex[i].y) + GEO_Y_MIN;
        max_key.x = ((x_size * (zindex[i].x + 1))) + GEO_X_MIN - 0.001;
        max_key.y = ((y_size * (zindex[i].y + 1))) + GEO_Y_MIN - 0.001;

        //cout << "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" << endl;
        //cout << i << ".  {" << zindex[i].x  << "  " << zindex[i].y << "}  ||  ("<< min_key.x << "  " << min_key.y << ")  ||  (" << max_key.x << "  " << max_key.y <<")" << endl;

        measure = Compute_Overlap(lb, ub, min_key, max_key);

        if (measure > 0) {
            Q_filter.insert(i);
        }
    }

    return Q_filter;
}

void LSMTree::reset_Q_filter() {
    // When move to next level, Reset query filter in range Query
    Q_filter.clear();
}

float LSMTree::Compute_Overlap(VAL_t Lower, VAL_t Upper, VAL_t min_key, VAL_t max_key)
{
    float dist_x = 0; float dist_y = 0;
    float measure = 0;

    dist_x = min(Upper.x - min_key.x, max_key.x - Lower.x);                                 // #include <algorithm>
    dist_y = min(Upper.y - min_key.y, max_key.y - Lower.y);

    if (dist_x > 0 && dist_y > 0) {
        measure = dist_x * dist_y;
    }

    return measure;

}

void LSMTree::range_query(entry_t query_point, float distance)
{
    vector<entry_t> range_result;

    entry_t Lower; entry_t Upper;                                   // Lower, Upper of query_point's Query Range

    vector<Level>::iterator current_level;
    current_level = levels.begin();

    Lower.val.x = query_point.val.x - distance; Lower.val.y = query_point.val.y - distance;
    Upper.val.x = query_point.val.x + distance; Upper.val.y = query_point.val.y + distance;

    Lower.key = make_key(Lower.val.x, Lower.val.y);
    Upper.key = make_key(Upper.val.x, Upper.val.y);

    set<entry_t> disk_entries;

    //////////// [ Range Query in Memory ] ////////////     
    cout << "  " << endl;
    cout << "* Buffer Result " << endl;
    cout << "-------------------------------------------------------------------------------------" << endl;
    cout << "[   X             Y             KEY          Distance ]" << endl;

    cout << left;
    for (const auto& entry : buffer.entries)
    {
        if (Compute_distance(query_point, entry) < distance) {
                range_result.push_back(entry);
            cout << setw(9) << entry.val.x << "  |  " << setw(9) << entry.val.y << "  |  " << setw(11) << entry.key << "  |  " << setw(9) << Compute_distance(query_point, entry) << endl;
        }
    }    

    set<int> Q_filter = Create_Query_filter(query_point.val, distance);
    set<int>::iterator iter;

    /* // Check a number of Q_filter
    for (iter = Q_filter.begin(); iter != Q_filter.end(); iter++) {
        cout << *iter << endl;
    }*/

    //////////// [ Range Query in Disk Level ] //////////// 
    for (int i = 0; i < DEFAULT_TREE_DEPTH; i++)
    {
        cout << "  " << endl;
        cout << "* Disk Level " << i + 1 << " Result " << endl;
        cout << "-------------------------------------------------------------------------------------" << endl;
        cout << "[   X             Y             KEY          Distance         Run Index ]" << endl;

        if (i < 3) {
            int temp = 0;
            for (iter = Q_filter.begin(); iter != Q_filter.end(); iter++) {
                int check_run = (*iter) / pow(4, 2 - i);                        // Matchting {Q_filter size and Full size}

                if (check_run != temp) {
                    // Disk Run Filtering by using Q_filter
                    if (current_level->runs_list[check_run]->spatial_filter[0] == 0 && current_level->runs_list[check_run]->spatial_filter[1] == 0 && current_level->runs_list[check_run]->spatial_filter[2] == 0 && current_level->runs_list[check_run]->spatial_filter[3] == 0) {
                        continue;
                    }

                    disk_entries = current_level->runs_list[check_run]->entries;
                    // Insert entry to Result
                    for (const auto& entry : disk_entries)
                    {
                        if (Compute_distance(query_point, entry) < distance) {
                            range_result.push_back(entry);
                            cout << setw(9) << entry.val.x << "  |  " << setw(9) << entry.val.y << "  |  " << setw(11) << entry.key << "  |  " << setw(9) << Compute_distance(query_point, entry) << "      | run index : " << check_run << endl;
                        }
                    }
                    temp = check_run;
                    cout << "-------------------------------------------------------------------------------------" << endl;
                }
            }    
            current_level += 1;
        }
        // Disk Level 4 ~
        else {
            for (iter = Q_filter.begin(); iter != Q_filter.end(); iter++) {
                int check_run = (*iter);

                // Disk Run Filtering by using Q_filter
                if (current_level->runs_list[check_run]->spatial_filter[0] == 0 && current_level->runs_list[check_run]->spatial_filter[1] == 0 && current_level->runs_list[check_run]->spatial_filter[2] == 0 && current_level->runs_list[check_run]->spatial_filter[3] == 0) {
                    continue;
                }

                disk_entries = current_level->runs_list[check_run]->entries;
                // Insert entry to Result
                for (const auto& entry : disk_entries)
                {
                    if (Compute_distance(query_point, entry) < distance) {
                        range_result.push_back(entry);
                        cout << setw(9) << entry.val.x << "  |  " << setw(9) << entry.val.y << "  |  " << setw(11) << entry.key << "  |  " << setw(9) << Compute_distance(query_point, entry) << "      | run index : " << check_run << endl;
                    }
                }
                cout << "-------------------------------------------------------------------------------------" << endl;
            }    
            current_level += 1;
        }        
    }
}

float LSMTree::Compute_distance(entry_t query_point, entry_t point) {
    float distance;

    distance = sqrt(pow(point.val.x - query_point.val.x, 2) + pow(point.val.y - query_point.val.y, 2));
    return distance;
}

//// [ Top - Down ] ////
void LSMTree::KNN_query1(entry_t query_point, int k)
{
    entry_t entry;
    set<pair<float, entry_t>> k_result;                             // k Result set
    float distance = 0;
    float compute_d = 8.656678713232676;
    set<int> Q_filter;
    set<entry_t>::iterator l_k; set<entry_t>::iterator u_k;
    set<entry_t>::iterator k_temp;

    set<pair<float, entry_t>> temp_result;
    set<int>::iterator Q_iter;
    set<pair<float, entry_t>>::iterator iter_result;

    l_k = u_k = buffer.entries.lower_bound(query_point);

    // 앞뒤로 k/2씩 이동해서 k개 범위 도출 
    for (int i = 0; i <= k; i++) {
        if (compute_d > Compute_distance(query_point, *l_k)) {
            compute_d = Compute_distance(query_point, *l_k);
            k_temp = l_k;
        }
        if (compute_d > Compute_distance(query_point, *u_k)) {
            compute_d = Compute_distance(query_point, *u_k);
            k_temp = u_k;
        }
        l_k--;
        u_k++;
    }

    // 2/k만큼 query_point로부터 가까운 진짜 LB, UB설정
    l_k = u_k = k_temp;
    for (int i = 0; i <= k; i++) {
        temp_result.insert({ Compute_distance(query_point, *u_k) ,*u_k });
        temp_result.insert({ Compute_distance(query_point, *l_k) ,*l_k });
        l_k--;
        u_k++;
    }

    //// [ Buffer ] 앞뒤 2/k 만큼 돌려서 나온 최종 k개 결과값을 k_result에 input //// 
    set<pair<float, entry_t>>::iterator temp_set;

    for (iter_result = temp_result.begin(); iter_result != temp_result.end(); iter_result++) {
        if (k_result.size() < k) {
            k_result.insert({ Compute_distance(query_point, (*iter_result).second), (*iter_result).second });
        }
        else {
            temp_set = k_result.end();
            temp_set--;
            distance = (*temp_set).first;
            temp_result.clear();
            break;
        }
    }

    Q_filter = Create_Query_filter(query_point.val, distance);

    //// [ Disk ] 앞뒤 2/k 만큼 돌려서 나온 최종 k개 결과값을 k_result에 input //// 
    for (int i = 0; i < DEFAULT_TREE_DEPTH; i++) {
        if (i < 3) {
            int temp = 0;
            for (Q_iter = Q_filter.begin(); Q_iter != Q_filter.end(); Q_iter++) {
                int check_run = (*Q_iter) / pow(4, 2 - i);
                if (check_run != temp) {

                    if (levels[i].runs_list[check_run]->spatial_filter[0] == 0 && levels[i].runs_list[check_run]->spatial_filter[1] == 0 && levels[i].runs_list[check_run]->spatial_filter[2] == 0 && levels[i].runs_list[check_run]->spatial_filter[3] == 0) {
                        continue;
                    }

                    l_k = u_k = levels[i].runs_list[check_run]->entries.lower_bound(query_point);
                    // 앞뒤로 k/2씩 이동해서 k개 출력
                    compute_d = 8.656678713232676;
                    for (int i = 0; i <= k; i++) {
                        if (compute_d > Compute_distance(query_point, *l_k)) {
                            compute_d = Compute_distance(query_point, *l_k);
                            k_temp = l_k;
                        }
                        if (compute_d > Compute_distance(query_point, *u_k)) {
                            compute_d = Compute_distance(query_point, *u_k);
                            k_temp = u_k;
                        }
                        l_k--;
                        u_k++;
                    }
                    l_k = u_k = k_temp;
                    compute_d = 0;
                    for (int i = 0; i <= k; i++) {
                        temp_result.insert({ Compute_distance(query_point, *u_k) ,*u_k });
                        temp_result.insert({ Compute_distance(query_point, *l_k) ,*l_k });
                        l_k--;
                        u_k++;
                    }

                    // compute_d만큼 range를 돌려서 결과를 temp_result에 input
                    // range query <-- level, idx, query_point, distance
                    //cout << compute_d << endl;
                    //temp_result = NN_range(i, check_run, query_point, compute_d);

                    for (iter_result = temp_result.begin(); iter_result != temp_result.end(); iter_result++) {
                        if ((*iter_result).first < distance) {
                            k_result.insert(*iter_result);

                            if (k_result.size() > k) {
                                temp_set = k_result.end();
                                temp_set--;
                                k_result.erase(*temp_set);
                            }

                            temp_set = k_result.end();
                            temp_set--;
                            distance = (*temp_set).first;
                        }
                        else {
                            temp_result.clear();
                            break;
                        }
                    }

                    temp = check_run;
                }
            }
        } // Disk Level 3 ~
        else {
            for (Q_iter = Q_filter.begin(); Q_iter != Q_filter.end(); Q_iter++) {
                int check_run = (*Q_iter);

                if (levels[i].runs_list[check_run]->spatial_filter[0] == 0 && levels[i].runs_list[check_run]->spatial_filter[1] == 0 && levels[i].runs_list[check_run]->spatial_filter[2] == 0 && levels[i].runs_list[check_run]->spatial_filter[3] == 0) {
                    continue;
                }

                l_k = u_k = levels[i].runs_list[check_run]->entries.lower_bound(query_point);
                // 앞뒤로 k/2씩 이동해서 k개 출력
                compute_d = 8.656678713232676;
                for (int i = 0; i <= k; i++) {
                    if (compute_d > Compute_distance(query_point, *l_k)) {
                        compute_d = Compute_distance(query_point, *l_k);
                        k_temp = l_k;
                    }
                    if (compute_d > Compute_distance(query_point, *u_k)) {
                        compute_d = Compute_distance(query_point, *u_k);
                        k_temp = u_k;
                    }
                    l_k--;
                    u_k++;
                }
                l_k = u_k = k_temp;
                for (int i = 0; i <= k; i++) {
                    temp_result.insert({ Compute_distance(query_point, *u_k) ,*u_k });
                    temp_result.insert({ Compute_distance(query_point, *l_k) ,*l_k });
                    l_k--;
                    u_k++;
                }

                // compute_d만큼 range를 돌려서 결과를 temp_result에 input
                // range query <-- level, idx, query_point, distance
                //temp_result = NN_range(i, check_run, query_point, compute_d);

                for (iter_result = temp_result.begin(); iter_result != temp_result.end(); iter_result++) {
                    if ((*iter_result).first < distance) {
                        k_result.insert(*iter_result);

                        if (k_result.size() > k) {
                            temp_set = k_result.end();
                            temp_set--;
                            k_result.erase(*temp_set);
                        }

                        temp_set = k_result.end();
                        temp_set--;
                        distance = (*temp_set).first;
                    }
                    else {
                        temp_result.clear();
                        break;
                    }
                }
            }
        }
    }

    //return k_result;
    cout << "  " << endl;
    cout << "* kNN Result " << endl;
    cout << "-------------------------------------------------------------------------------------" << endl;
    cout << "[     X             Y             KEY          Distance ]" << endl;
    cout << left;
    for (auto it = k_result.begin(); it != k_result.end(); it++) {
        cout << setw(9) << (*it).second.val.x << "  |  " << setw(9) << (*it).second.val.y << "  |  " << setw(11) << (*it).second.key << "  |  " << setw(9) << (*it).first << endl;
    }
}

//// [ Bottom - Up ] ////
void LSMTree::KNN_query2(entry_t query_point, int k) {
    entry_t entry;
    set<pair<float, entry_t>> k_result;                             // k Result set
    float distance = 0;
    float compute_d = 8.656678713232676;
    set<int> Q_filter;
    set<entry_t>::iterator l_k; set<entry_t>::iterator u_k;
    set<entry_t>::iterator k_temp;

    set<pair<float, entry_t>> temp_result;
    set<int>::iterator Q_iter;
    set<pair<float, entry_t>>::iterator iter_result;

    l_k = u_k = buffer.entries.lower_bound(query_point);

    // 앞뒤로 k/2씩 이동해서 k개 범위 도출 
    for (int i = 0; i <= k; i++) {
        if (compute_d > Compute_distance(query_point, *l_k)) {
            compute_d = Compute_distance(query_point, *l_k);
            k_temp = l_k;
        }
        if (compute_d > Compute_distance(query_point, *u_k)) {
            compute_d = Compute_distance(query_point, *u_k);
            k_temp = u_k;
        }
        l_k--;
        u_k++;
    }

    // 2/k만큼 query_point로부터 가까운 진짜 LB, UB설정
    l_k = u_k = k_temp;
    for (int i = 0; i <= k; i++) {
        temp_result.insert({ Compute_distance(query_point, *u_k) ,*u_k });
        temp_result.insert({ Compute_distance(query_point, *l_k) ,*l_k });
        l_k--;
        u_k++;
    }

    //// [ Buffer ] 앞뒤 2/k 만큼 돌려서 나온 최종 k개 결과값을 k_result에 input //// 
    set<pair<float, entry_t>>::iterator temp_set;

    for (iter_result = temp_result.begin(); iter_result != temp_result.end(); iter_result++) {
        if (k_result.size() < k) {
            k_result.insert({ Compute_distance(query_point, (*iter_result).second), (*iter_result).second });
        }
        else {
            temp_set = k_result.end();
            temp_set--;
            distance = (*temp_set).first;
            temp_result.clear();
            break;
        }
    }

    Q_filter = Create_Query_filter(query_point.val, distance);

    //// [ Disk ] 앞뒤 2/k 만큼 돌려서 나온 최종 k개 결과값을 k_result에 input //// 
    for (int i = DEFAULT_TREE_DEPTH-1; i >= 0; i--) {
        if (i < 3) {
            int temp = 0;
            for (Q_iter = Q_filter.begin(); Q_iter != Q_filter.end(); Q_iter++) {
                int check_run = (*Q_iter) / pow(4, 2 - i);
                if (check_run != temp) {

                    if (levels[i].runs_list[check_run]->spatial_filter[0] == 0 && levels[i].runs_list[check_run]->spatial_filter[1] == 0 && levels[i].runs_list[check_run]->spatial_filter[2] == 0 && levels[i].runs_list[check_run]->spatial_filter[3] == 0) {
                        continue;
                    }

                    l_k = u_k = levels[i].runs_list[check_run]->entries.lower_bound(query_point);
                    // 앞뒤로 k/2씩 이동해서 k개 출력
                    compute_d = 8.656678713232676;
                    for (int i = 0; i <= k; i++) {
                        if (compute_d > Compute_distance(query_point, *l_k)) {
                            compute_d = Compute_distance(query_point, *l_k);
                            k_temp = l_k;
                        }
                        if (compute_d > Compute_distance(query_point, *u_k)) {
                            compute_d = Compute_distance(query_point, *u_k);
                            k_temp = u_k;
                        }
                        l_k--;
                        u_k++;
                    }
                    l_k = u_k = k_temp;
                    compute_d = 0;
                    for (int i = 0; i <= k; i++) {
                        temp_result.insert({ Compute_distance(query_point, *u_k) ,*u_k });
                        temp_result.insert({ Compute_distance(query_point, *l_k) ,*l_k });
                        l_k--;
                        u_k++;
                    }

                    // compute_d만큼 range를 돌려서 결과를 temp_result에 input
                    // range query <-- level, idx, query_point, distance
                    //cout << compute_d << endl;
                    //temp_result = NN_range(i, check_run, query_point, compute_d);

                    for (iter_result = temp_result.begin(); iter_result != temp_result.end(); iter_result++) {
                        if ((*iter_result).first < distance) {
                            k_result.insert(*iter_result);

                            if (k_result.size() > k) {
                                temp_set = k_result.end();
                                temp_set--;
                                k_result.erase(*temp_set);
                            }

                            temp_set = k_result.end();
                            temp_set--;
                            distance = (*temp_set).first;
                        }
                        else {
                            temp_result.clear();
                            break;
                        }
                    }

                    temp = check_run;
                }
            }
        } // Disk Level 3 ~
        else {
            for (Q_iter = Q_filter.begin(); Q_iter != Q_filter.end(); Q_iter++) {
                int check_run = (*Q_iter);

                if (levels[i].runs_list[check_run]->spatial_filter[0] == 0 && levels[i].runs_list[check_run]->spatial_filter[1] == 0 && levels[i].runs_list[check_run]->spatial_filter[2] == 0 && levels[i].runs_list[check_run]->spatial_filter[3] == 0) {
                    continue;
                }

                l_k = u_k = levels[i].runs_list[check_run]->entries.lower_bound(query_point);
                // 앞뒤로 k/2씩 이동해서 k개 출력
                compute_d = 8.656678713232676;
                for (int i = 0; i <= k; i++) {
                    if (compute_d > Compute_distance(query_point, *l_k)) {
                        compute_d = Compute_distance(query_point, *l_k);
                        k_temp = l_k;
                    }
                    if (compute_d > Compute_distance(query_point, *u_k)) {
                        compute_d = Compute_distance(query_point, *u_k);
                        k_temp = u_k;
                    }
                    l_k--;
                    u_k++;
                }
                l_k = u_k = k_temp;
                for (int i = 0; i <= k; i++) {
                    temp_result.insert({ Compute_distance(query_point, *u_k) ,*u_k });
                    temp_result.insert({ Compute_distance(query_point, *l_k) ,*l_k });
                    l_k--;
                    u_k++;
                }

                // compute_d만큼 range를 돌려서 결과를 temp_result에 input
                // range query <-- level, idx, query_point, distance
                //temp_result = NN_range(i, check_run, query_point, compute_d);

                for (iter_result = temp_result.begin(); iter_result != temp_result.end(); iter_result++) {
                    if ((*iter_result).first < distance) {
                        k_result.insert(*iter_result);

                        if (k_result.size() > k) {
                            temp_set = k_result.end();
                            temp_set--;
                            k_result.erase(*temp_set);
                        }

                        temp_set = k_result.end();
                        temp_set--;
                        distance = (*temp_set).first;
                    }
                    else {
                        temp_result.clear();
                        break;
                    }
                }
            }
        }
    }

    //return k_result;
    cout << "  " << endl;
    cout << "* kNN Result " << endl;
    cout << "-------------------------------------------------------------------------------------" << endl;
    cout << "[     X             Y             KEY          Distance ]" << endl;
    cout << left;
    for (auto it = k_result.begin(); it != k_result.end(); it++) {
        cout << setw(9) << (*it).second.val.x << "  |  " << setw(9) << (*it).second.val.y << "  |  " << setw(11) << (*it).second.key << "  |  " << setw(9) << (*it).first << endl;
    }
}

// range query <-- level, idx, query_point, distance
set<pair<float, entry_t>> LSMTree::NN_range(int current, int idx, entry_t query_point, float distance) {
    set<pair<float, entry_t>> run_result;
    set<entry_t> disk_entries;

    disk_entries = levels[current].runs_list[idx]->entries;

    for (const auto& entry : disk_entries)
    {
        if (Compute_distance(query_point, entry) < distance) {
            run_result.insert(make_pair(Compute_distance(query_point, entry), entry));
        }
    }

    return run_result;
}