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
            int temp = 4294967295 / 4;
            for (int i = 0; i < 4; i++) {
                min_key = temp * i;
                max_key = temp * (i + 1) - 1;
                Run* tmp1 = new Run(current->max_run_size, bf_bits_per_entry, max_key, min_key, 1, i);
                current->runs_list[i] = tmp1;
            }
        }
        else if (cnt == 2) {
            int temp = 4294967295 / 16;
            for (int i = 0; i < 16; i++) {
                min_key = temp * i;
                max_key = temp * (i + 1) - 1;
                Run* tmp1 = new Run(current->max_run_size, bf_bits_per_entry, max_key, min_key, 2, i);
                current->runs_list[i] = tmp1;
            }
        }
        else {
            int temp = 4294967295 / 64;
            for (int i = 0; i < 64; i++) {
                min_key = temp * i;
                max_key = temp * (i + 1) - 1;
                Run* tmp1 = new Run(current->max_run_size, bf_bits_per_entry, max_key, min_key, cnt, i);
                current->runs_list[i] = tmp1;
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
        cout << "~~~~~~~~~~start merge down~~~~~~~~~~" << endl;
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
    temp = 4294967295 / pow(4, current->runs_list[idx]->idx_level);
    if (current->runs_list[idx]->idx_level < 3) {
        //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // generate file
        st_file_name = "runs_list/" + to_string(level_temp) + "_" + to_string(i) + ".txt";
        cout << st_file_name << endl;
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
                cout << st_file_name << endl;
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
            next->runs_list[i]->spatial_filter[(entry.key - next->runs_list[i]->min_key) / (temp / 4)] = 1;
            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // input to txt file
            input_data = to_string(entry.val.x) + "  " + to_string(entry.val.y) + "  " + to_string(entry.key) + "\n";
            //cout << input_data << endl;
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
        cout << st_file_name << endl;
        strcpy(ch_file_name, st_file_name.c_str());
        fp = fopen(ch_file_name, "w"); //test파일을 w(쓰기) 모드로 열기
        //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        for (const auto& entry : current->runs_list[idx]->entries) {
            next->runs_list[idx]->put(entry);
            next->runs_list[idx]->spatial_filter[(entry.key - next->runs_list[idx]->min_key) / (temp / 4)] = 1;
            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // input to txt file
            input_data = to_string(entry.val.x) + "  " + to_string(entry.val.y) + "  " + to_string(entry.key) + "\n";
            //cout << input_data << endl;
            strcpy(ch_input_data, input_data.c_str());
            fputs(ch_input_data, fp); //문자열 입력
            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        }
        fclose(fp);       //파일 포인터 닫기//////////////////////////////////////////////////////////////////////<------------this 
    }
    cout << "step4" << endl;

    /*
     * if the next level does not have space for the current level,
     * recursively merge the next level downwards to create some
     */

    cout << "step1" << endl;

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
     * Clear the current level to delete the old (now
     * redundant) entry files.
     */
    KEY_t min_temp = current->runs_list[idx]->min_key;
    KEY_t max_temp = current->runs_list[idx]->max_key;
    Run* tmp = new Run(current->max_run_size, bf_bits_per_entry, max_temp, min_temp, level_temp - 1, idx);

    current->runs_list[idx]->empty();
    //delete current->runs_list[idx];

    cout << "step5" << endl;
    current->runs_list[idx] = tmp;
    cout << "~~~~~~~~~~end merge down~~~~~~~~~~" << endl;
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
     * If the buffer is full, flush level 0 if necessary
     * to create space
     */
    cout << "step put1" << endl;

    for (int i = 0; i < 4; i++) {
        if (levels.front().runs_list[i] != NULL) {
            merge_down(levels.begin(), i);
        }
    }
    cout << "step put2" << endl;

    /*
     * Flush the buffer to level 0
     */
    int i = 0; KEY_t temp = 4294967295;                  // 'i' is a run index of levels
    min_key = 0;
    max_key = temp / 4 - 1;
    int loop = 0;

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // generate file    
    string st_file_name;        
    char ch_file_name[100];
    st_file_name = "runs_list/" + to_string(levels.front().runs_list[i]->idx_level) + "_" + to_string(i) + ".txt";
    cout << st_file_name << endl;
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
            cout << st_file_name << endl;
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
        levels.front().runs_list[i]->spatial_filter[(entry.key - levels.front().runs_list[i]->min_key) / (temp / 16)] = 1;

        //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // input to txt file                
        input_data = to_string(entry.val.x) + "  " + to_string(entry.val.y) + "  " + to_string(entry.key) + "\n";
        //cout << input_data << endl;
        strcpy(ch_input_data, input_data.c_str());
        fputs(ch_input_data, fp); //문자열 입력        
        //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    }

    fclose(fp);      //파일 포인터 닫기/////////////////////////////////////////////////////<-------얘도

    cout << "part1 \n";

    buffer.empty();
    cout << buffer.entries.size() << endl;

    cout << "part2 \n";
    
    assert(buffer.put(key, val));
    cout << "part3 \n";
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
    for (int i = 15; i >= 0; i--)
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
