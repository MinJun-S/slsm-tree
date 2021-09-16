#include <cassert>
#include <fstream>
#include <iostream>
#include <map>
#include <cmath>

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
    long max_run_size;

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
}

// run input
// vector<Run> runs;
void LSMTree::merge_down(vector<Level>::iterator current, int idx) {
    vector<Level>::iterator next;
    Run** runs_list;
    MergeContext merge_ctx;
    MergeContext merge_temp;
    entry_t entry;

    KEY_t temp;

    KEY_t max_key;
    KEY_t min_key;
  
    assert(current >= levels.begin());

    if (current->runs_list[idx]->remaining() > 0) {
        return;
    }
    else if (current >= levels.end() - 1) {
        die("no more space in tree.");
    }
    else {
        next = current + 1;
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
     * Merge all runs in the current level into the first
     * run in the next level
     */

    merge_ctx.add(current->runs_list[idx]->map_read(), current->runs_list[idx]->size);

    // run ÁöÁ¤ºÎºÐ¿¡ Ãß°¡
    if (current->runs_list[idx]->idx_level < 3) {
        for (int i = idx * 4; i <= idx * 4 + 3; i++) {
            if (next->runs_list[i] != NULL) {
                merge_temp.add(next->runs_list[i]->map_read(), current->runs_list[idx]->size);
            }
            else {
                temp = 4294967295 / pow(4, distance(levels.begin(), next));
                max_key = temp * (i + 1);
                min_key = temp * i;

                Run tmp = Run(next->max_run_size, bf_bits_per_entry, max_key, min_key);
                next->runs_list[i] = &tmp;
                next->runs_list[i]->map_write();

                //next.runs_list[i](next->max_run_size, bf_bits_per_entry, max_key, min_key);
                //next.runs_list[i].map_write();
            }
            while (!merge_ctx.done()) {
                entry = merge_ctx.next();

                // Remove deleted keys from the final level
                if (entry.key <= next->runs_list[i]->max_key && entry.key > next->runs_list[i]->min_key)
                {
                    next->runs_list[i]->put(entry);
                }
            }
            next->runs_list[i]->unmap();
        }
    }
    else {
        if (next->runs_list[idx] != NULL) {
            merge_temp.add(next->runs_list[idx]->map_read(), current->runs_list[idx]->size);
        }
        else {
            max_key = current->runs_list[idx]->max_key;
            min_key = current->runs_list[idx]->min_key;

            Run tmp = Run(next->max_run_size, bf_bits_per_entry, max_key, min_key);
            next->runs_list[idx] = &tmp;
            next->runs_list[idx]->map_write();
        }
        while (!merge_ctx.done()) {
            entry = merge_ctx.next();
            next->runs_list[idx]->put(entry);
        }
        next->runs_list[idx]->unmap();
    }

    //next->runs.emplace_front(next->max_run_size, bf_bits_per_entry);
    //next->runs.front().map_write();

    current->runs_list[idx]->unmap();

    /*
     * Clear the current level to delete the old (now
     * redundant) entry files.
     */

    delete current->runs_list[idx];
}

//void lsmtree::merge_down(vector<level>::iterator current) {
//    vector<level>::iterator next;
//    mergecontext merge_ctx;
//    entry_t entry;
//
//    assert(current >= levels.begin());
//
//    if (current->remaining() > 0) {
//        return;
//    } else if (current >= levels.end() - 1) {
//        die("no more space in tree.");
//    } else {
//        next = current + 1;
//    }
//
//    /*
//     * if the next level does not have space for the current level,
//     * recursively merge the next level downwards to create some
//     */
//
//    if (next->remaining() == 0) {
//        merge_down(next);
//        assert(next->remaining() > 0);
//    }
//
//    /*
//     * merge all runs in the current level into the first
//     * run in the next level
//     */
//
//    for (auto& run : current->runs) {
//        merge_ctx.add(run.map_read(), run.size);
//    }
//
//    next->runs.emplace_front(next->max_run_size, bf_bits_per_entry);
//    next->runs.front().map_write();
//
//    while (!merge_ctx.done()) {
//        entry = merge_ctx.next();
//
//        // remove deleted keys from the final level
//        if (!(next == levels.end() - 1 && (entry.val.y == val_tombstone|| entry.val.x )))
//        {
//            next->runs.front().put(entry);
//        }
//    }
//
//    next->runs.front().unmap();
//
//    for (auto& run : current->runs) {
//        run.unmap();
//    }
//
//    /*
//     * clear the current level to delete the old (now
//     * redundant) entry files.
//     */
//
//    current->runs.clear();
//}

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

    merge_down(levels.begin(), 0);

    /*
     * Flush the buffer to level 0
     */
    int i = 0; KEY_t temp = 4294967295;
    
    for (const auto& entry : buffer.entries) 
    {
        if (entry.key >= 0 && entry.key< temp/4)
        {
            i = 0;
            min_key = 0;
            max_key = temp / 4 - 1;                 
        }
        else if (entry.key >= temp / 4 && entry.key < temp/2)
        {
            i = 1;
            min_key = temp / 4;
            max_key = temp / 2 -1;
        }
        else if (entry.key >= temp / 2 && entry.key < (temp / 4) * 3)
        {
            i = 2;
            min_key = temp / 2;
            max_key = (temp / 4) * 3 - 1;
        }
        else
        {
            i = 3;
            min_key = (temp / 4) * 3;
            max_key = temp;
        }

        if (levels.front().runs_list[i] == NULL)
        {
            Run tmp = Run(levels.front().max_run_size, bf_bits_per_entry, max_key, min_key);
            levels.front().runs_list[i] = &tmp;
        }
        levels.front().runs_list[i]->map_write();
        levels.front().runs_list[i]->put(entry);
           
        levels.front().runs.front().unmap();
    }


    /*
    levels.front().runs.emplace_front(levels.front().max_run_size, bf_bits_per_entry);
    levels.front().runs.front().map_write();

    for (const auto& entry : buffer.entries) {
        levels.front().runs.front().put(entry);
    }

    levels.front().runs.front().unmap();
    */
    /*
     * Empty the buffer and insert the key/value pair
     */

    buffer.empty();
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