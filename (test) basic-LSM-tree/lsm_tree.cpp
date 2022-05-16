/*
	실험용 basic LSM 코드 수정본
*/
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

	// 우리코드에서 basic으로 바꿈 : 두 배가 되도록 (사분할하는거 지우자)
    while ((depth--) > 0) {
        /*if (fanout < 64) {
            fanout *= 4;
        }
        else {
            max_run_size *= 2;
        }*/
		max_run_size *= 2;
        levels.emplace_back(fanout, max_run_size);
    }

	
    current = levels.begin();
	/*vector<Level>::iterator eeeeeee;
	eeeeeee = levels.end();*/
	//cout << "level~~~> " << (*current) << "      " << (*eeeeeee) << endl;

    int cnt = 0;
	// 우리코드에서 basic으로 바꿈 : class Run 먼저 바꾸고 오자, minkey이런거 다 없애기
    while ((depth_run--) > 0) {
        cnt += 1;
		Run* tmp1 = new Run(current->max_run_size, bf_bits_per_entry, cnt);
		current->runs_list[0] = tmp1;

		cout << "Level~~~> " << current->runs_list[0]->idx_level << endl;
        current += 1;		
    }
	
}

// idx 관련된거 다 지워버리고? 필터랑 minkey랑
void LSMTree::merge_down(vector<Level>::iterator current, int idx) {
    vector<Level>::iterator next;
    Run** runs_list;
    MergeContext merge_ctx;
    entry_t entry;

    KEY_t temp;

    KEY_t max_key;
    KEY_t min_key;
  
    assert(current >= levels.begin());
    //cout << "step0_assert" << endl;

    if (current->runs_list[idx]->remaining() > 0) {
		// enough space in next runs list of current level, then  
        return;
    }
    else if (current >= levels.end() - 1) {
		// 
        die("no more space in tree(= maximum level.");
    }
    else {
		// Flush (=no more space in all runs_list of current level)
        //cout << "\n ┏------------- [ START : Merge Down ] -------------┓\n" << endl;
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
    int i = 0;
    FILE* fp = NULL;

    //////////////////////////////////////////////////////////////////////////////////
    // generate file
    st_file_name = "runs_list/" + to_string(level_temp) + "_" + to_string(i) + ".txt";   /////////////////////////////////////여기 idx_level이 아니고 level_temp를 왜 쓰는거지?????
    strcpy(ch_file_name, st_file_name.c_str());
    fp = fopen(ch_file_name, "w"); 
    //////////////////////////////////////////////////////////////////////////////////


    max_key = next->runs_list[i]->max_key;

	if (next->runs_list[i]->entries.begin()->key != 0) {
		IO_Check = IO_Check + 2 * int(current->runs_list[i]->entries.size() / DEFAULT_BUFFER_NUM_PAGES) + 2 * int(next->runs_list[i]->entries.size() / DEFAULT_BUFFER_NUM_PAGES);    // 다음레벨에 있으면 한번 보고(+1) 레벨에 따라 2배수 더해줌(+{다음레벨 들어있는 양/버퍼사이즈}) 몫
		if (next->runs_list[i]->entries.size() % DEFAULT_BUFFER_NUM_PAGES > 0) {						// {다음레벨 들어있는 양/버퍼사이즈} 해준게 딱 나눠 떨어지지 않기 때문에,
			IO_Check = IO_Check + 2;																	// 자투리에 조금이라도 남아있을 수 있어서 +1해줌
		}
        if (current->runs_list[i]->entries.size() % DEFAULT_BUFFER_NUM_PAGES > 0) {						// {다음레벨 들어있는 양/버퍼사이즈} 해준게 딱 나눠 떨어지지 않기 때문에,
            IO_Check = IO_Check + 2;																	// 자투리에 조금이라도 남아있을 수 있어서 +1해줌
        }
	}

    for (const auto& entry : current->runs_list[idx]->entries) 
    {
        /*
        entry_t tmp;
        tmp.key = entry.key; tmp.val = entry.val;
        levels.front().runs_list[i]->put(tmp);
        */
        next->runs_list[i]->put(entry);

		string fileName = "src/DBFiles/";
		fileName += to_string(entry.key) + ".txt";
		FILE* filePtr = fopen(fileName.c_str(), "w");
		string userTuple = to_string(entry.key) + "\n";
		fprintf(filePtr, userTuple.c_str());
		next->runs_list[i]->bPTree->bpt_insert(entry.key, filePtr);
		fclose(filePtr);

        //////////////////////////////////////////////////////////////////////////////
        // input to txt file
        input_data = to_string(entry.val.x) + "  " + to_string(entry.val.y) + "  " + to_string(entry.key) + "\n";
        strcpy(ch_input_data, input_data.c_str());
        fputs(ch_input_data, fp); //문자열 입력
        //////////////////////////////////////////////////////////////////////////////
    }
    fclose(fp);       //파일 포인터 닫기//////////////////////////////////////////////

    /*
     * if the next level does not have space for the current level,
     * recursively merge the next level downwards to create some
     */
    if (next->runs_list[i]->remaining() <= 0) {
        merge_down(next, i);
        assert(next->runs_list[i]->remaining() > 0);
    }
    
    /*
     * Clear the current level to delete the old (now redundant) entry files.
     */
    Run* tmp = new Run(current->max_run_size, bf_bits_per_entry, level_temp - 1);

	delete current->runs_list[idx]->bPTree;
    current->runs_list[idx]->empty();
    //delete current->runs_list[idx];

    
    current->runs_list[idx] = tmp;
    //cout << "\n ┗------------- [ END : Merge Down ] -------------┛\n" << endl;
}

void LSMTree::put(KEY_t key, VAL_t val, BPTree** bPTree) {
    KEY_t max_key;
    KEY_t min_key;
    /*
     * Try inserting the key into the buffer
     */
    if (buffer.put(key, val, bPTree)) {
        return;
    }

    /*
     * If the buffer is full, flush level 0 if necessary to create space
     */

    if (levels.front().runs_list[0] != NULL) {
        merge_down(levels.begin(), 0);
    }

    /*
     * Flush the buffer to level 0
     */
    int i = 0; KEY_t temp = KEY_MAX;                  // 'i' is a run index of levels
    min_key = 0;
    max_key = temp / 4 - 1;
    int loop = 0;

	vector<Level>::iterator current;
	current = levels.begin();

    ////////////////////////////////////////////////////////////////////////
    // generate file    
    string st_file_name;        
    char ch_file_name[100];
    st_file_name = "runs_list/" + to_string(levels.front().runs_list[i]->idx_level) + "_" + to_string(i) + ".txt";
    strcpy(ch_file_name, st_file_name.c_str());
    FILE* fp = NULL;
    fp = fopen(ch_file_name, "w"); 

    string input_data;
    char ch_input_data[100];
	/* Buffer -> level 1 flush */

    for (const auto& entry : buffer.entries) {
		levels.front().runs_list[i]->put(entry);

		string fileName = "src/DBFiles/";
		fileName += to_string(entry.key) + ".txt";
		FILE* filePtr = fopen(fileName.c_str(), "w");
		string userTuple = to_string(entry.key) + "\n";
		fprintf(filePtr, userTuple.c_str());
		levels.front().runs_list[i]->bPTree->bpt_insert(entry.key, filePtr);
		fclose(filePtr);

        // input to txt file
        input_data = to_string(entry.val.x) + "  " + to_string(entry.val.y) + "  " + to_string(entry.key) + "\n";
        strcpy(ch_input_data, input_data.c_str());
        fputs(ch_input_data, fp); //문자열 입력
    }

    IO_Check = IO_Check + 1;
    if (levels.front().runs_list[0]->entries.begin()->key != 0) {
        IO_Check = IO_Check + 2 * int(levels.front().runs_list[0]->entries.size() / DEFAULT_BUFFER_NUM_PAGES);
        if (levels.front().runs_list[0]->entries.size() % DEFAULT_BUFFER_NUM_PAGES > 0) {						// {다음레벨 들어있는 양/버퍼사이즈} 해준게 딱 나눠 떨어지지 않기 때문에,
            IO_Check = IO_Check + 2;																	// 자투리에 조금이라도 남아있을 수 있어서 +1해줌
        }
    }

    fclose(fp);       //파일 포인터 닫기////////////////////////////////////////

	//delete levels.front().runs_list[i]->bPTree;
    buffer.empty();
    //cout << buffer.entries.size() << endl;

    assert(buffer.put(key, val, bPTree));
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
    //put(key, temp);
}

// 원본 load() 함수 (지우지말것)
void LSMTree::load(string file_path) {
    ifstream stream;
    entry_t entry;

    stream.open(file_path, ifstream::binary);

    if (stream.is_open()) {
        while (stream >> entry) {
            //put(entry.key, entry.val);
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
    cout << "* Number of entry in Buffer = " << i << endl;


    set<entry_t> disk_entries;
    vector<Level>::iterator current_level;
    current_level = levels.begin();
	current_level->runs_list[0]->idx_level;

	//while ((*current_level)<3) {
	//	i = 0;
	//	disk_entries = current_level->runs_list[0]->entries;
	//	// Insert entry to Result
	//	for (const auto& entry : disk_entries)
	//	{
	//		cout << entry.val.x << " " << entry.val.y << " " << entry.key << "\n";
	//		i++;
	//	}
	//	//cout << "* Number of entry in Disk " << (*current_level) <<"_" << i << endl;
	//	current_level = current_level + 1;
	//}


}


void LSMTree::save_run()
{
	/*
    vector<Level>::iterator current;
    string st_file_name;
    char ch_file_name[100];
    string input_data;
    char ch_input_data[100];

    current = levels.begin();
    int idx = 0;
    FILE* fp = NULL;

	int i = 0;

    for (int j = 1; j <= levels.size(); j++) {
        cout << j << endl;
        
		if (current->runs_list[i] != NULL) {
			//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			// generate file
			st_file_name = "run_list/" + to_string(j) + "_" + to_string(i) + ".txt";
			cout << st_file_name << endl;
			strcpy(ch_file_name, st_file_name.c_str());
			fp = fopen(ch_file_name, "w"); //test파일을 w(쓰기) 모드로 열기
			//////////////////////////////////////////////////////////////////////////////////////////////////////////////////    
			for (const auto& entry : current->runs_list[i]->entries)
			{
				// input to txt file
				input_data = to_string(entry.val.x) + " " + to_string(entry.val.y) + " " + to_string(entry.key) + "\n";
				strcpy(ch_input_data, input_data.c_str());
				fputs(ch_input_data, fp); //문자열 입력
			}
			fclose(fp);       //파일 포인터 닫기//////////////////////////////////////////////////////////////////////<------------this 
		}
		else continue;           
        
		current = current + 1;
    }
	*/
}

// Save to File of All Data
void LSMTree::save_file()
{
    vector<Level>::iterator current;
    string st_file_name;
    char ch_file_name[100];
    string input_data;
    char ch_input_data[100];

    current = levels.end();
    current--;
    //current = levels.begin();
    int idx = 0;
    FILE* fp = NULL;

    st_file_name = "SaveFile/Save_File.txt";
    strcpy(ch_file_name, st_file_name.c_str());
    fp = fopen(ch_file_name, "w"); //test파일을 w(쓰기) 모드로 열기


    // Saving Disk Level
    //for (int j = 1; j <= levels.size(); j++) {
    for (int j = levels.size(); j >= 1; j--) {  //거꾸로 저장해야함
        if (current->runs_list[idx]->idx_level <= 3) {
            for (int i = 0; i < int(pow(4, j)); i++) {
                if (current->runs_list[i] != NULL) {

                    for (const auto& entry : current->runs_list[i]->entries)
                    {
                        // input to txt file
                        input_data = to_string(entry.val.x) + " " + to_string(entry.val.y) + " " + to_string(entry.key) + "\n";
                        strcpy(ch_input_data, input_data.c_str());
                        fputs(ch_input_data, fp); //문자열 입력
                    }
                    
                }
                else continue;
            }
        }
        else if (current->runs_list[idx]->idx_level > 3) {
            for (int i = 0; i < 64; i++) {
                if (current->runs_list[i] != NULL) {
                      
                    for (const auto& entry : current->runs_list[i]->entries)
                    {
                        // input to txt file
                        input_data = to_string(entry.val.x) + " " + to_string(entry.val.y) + " " + to_string(entry.key) + "\n";
                        strcpy(ch_input_data, input_data.c_str());
                        fputs(ch_input_data, fp); //문자열 입력
                    }
                    
                }
                else continue;
            }
        }
        else break;
        current--;
    }

    // Saving Buffer
    for (const auto& entry : buffer.entries)
    {
        // input to txt file
        input_data = to_string(entry.val.x) + " " + to_string(entry.val.y) + " " + to_string(entry.key) + "\n";
        strcpy(ch_input_data, input_data.c_str());
        fputs(ch_input_data, fp); //문자열 입력
    }

    fclose(fp);       //파일 포인터 닫기//////////////////////////////////////////////////////////////////////<------------this 
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
    cout << "\n* Buffer Result " << endl;
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

    set<int>::iterator iter;

    //////////// [ Range Query in Disk Level ] //////////// 
    for (int i = 0; i < DEFAULT_TREE_DEPTH; i++)
    {
        cout << "\n* Disk Level " << i + 1 << " Result " << endl;
        cout << "-------------------------------------------------------------------------------------" << endl;
        cout << "[   X             Y             KEY          Distance         Run Index ]" << endl;

        //disk_entries = current_level->runs_list[0]->entries;
        // Insert entry to Result
        disk_entries = levels[i].runs_list[0]->entries;
        for (const auto& entry : disk_entries)
        {
            if (Compute_distance(query_point, entry) < distance) {
                range_result.push_back(entry);
                cout << setw(9) << entry.val.x << "  |  " << setw(9) << entry.val.y << "  |  " << setw(11) << entry.key << "  |  " << setw(9) << Compute_distance(query_point, entry) << endl;
            }
        }

        IO_Check = IO_Check + int(levels[i].runs_list[0]->entries.size() / DEFAULT_BUFFER_NUM_PAGES);    // 다음레벨에 있으면 한번 보고(+1) 레벨에 따라 2배수 더해줌(+{다음레벨 들어있는 양/버퍼사이즈}) 몫
        if (levels[i].runs_list[0]->entries.size() % DEFAULT_BUFFER_NUM_PAGES > 0) {						// {다음레벨 들어있는 양/버퍼사이즈} 해준게 딱 나눠 떨어지지 않기 때문에,
            IO_Check = IO_Check + 1;																	// 자투리에 조금이라도 남아있을 수 있어서 +1해줌
        }

        cout << "-------------------------------------------------------------------------------------" << endl;
        //current_level += 1;
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

    //// [ Disk ] 앞뒤 2/k 만큼 돌려서 나온 최종 k개 결과값을 k_result에 input //// 
    for (int i = 0; i < DEFAULT_TREE_DEPTH; i++) {
        l_k = u_k = levels[i].runs_list[0]->entries.lower_bound(query_point);

        IO_Check = IO_Check + int(levels[i].runs_list[0]->entries.size() / DEFAULT_BUFFER_NUM_PAGES);    // 다음레벨에 있으면 한번 보고(+1) 레벨에 따라 2배수 더해줌(+{다음레벨 들어있는 양/버퍼사이즈}) 몫
        if (levels[i].runs_list[0]->entries.size() % DEFAULT_BUFFER_NUM_PAGES > 0) {						// {다음레벨 들어있는 양/버퍼사이즈} 해준게 딱 나눠 떨어지지 않기 때문에,
            IO_Check = IO_Check + 1;																	// 자투리에 조금이라도 남아있을 수 있어서 +1해줌
        }

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

    //return k_result;
    cout << "\n* kNN Result " << endl;
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

    //// [ Disk ] 앞뒤 2/k 만큼 돌려서 나온 최종 k개 결과값을 k_result에 input //// 
    for (int i = DEFAULT_TREE_DEPTH - 1; i >= 0; i--) {
        l_k = u_k = levels[i].runs_list[0]->entries.lower_bound(query_point);

        IO_Check = IO_Check + int(levels[i].runs_list[0]->entries.size() / DEFAULT_BUFFER_NUM_PAGES);    // 다음레벨에 있으면 한번 보고(+1) 레벨에 따라 2배수 더해줌(+{다음레벨 들어있는 양/버퍼사이즈}) 몫
        if (levels[i].runs_list[0]->entries.size() % DEFAULT_BUFFER_NUM_PAGES > 0) {						// {다음레벨 들어있는 양/버퍼사이즈} 해준게 딱 나눠 떨어지지 않기 때문에,
            IO_Check = IO_Check + 1;																	// 자투리에 조금이라도 남아있을 수 있어서 +1해줌
        }

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

    //return k_result;
    cout << "\n* kNN Result " << endl;
    cout << "-------------------------------------------------------------------------------------" << endl;
    cout << "[     X             Y             KEY          Distance ]" << endl;
    cout << left;
    for (auto it = k_result.begin(); it != k_result.end(); it++) {
        cout << setw(9) << (*it).second.val.x << "  |  " << setw(9) << (*it).second.val.y << "  |  " << setw(11) << (*it).second.key << "  |  " << setw(9) << (*it).first << endl;
    }
}

/* B+Tree Range 쿼리 */
void LSMTree::searchMethod(BPTree* bPTree, KEY_t Lower, KEY_t Upper) {
	int bpt_IO_Check = 0;	

	// IO체크할라고 disk에 있는 B+Tree만 작업했음
	BPTree* disk_bPTree;
	for (int i = 0; i < DEFAULT_TREE_DEPTH; i++)
	{
		cout << "\n* Disk Level " << i + 1 << " Result " << endl;
		cout << "-------------------------------------------------------------------------------------" << endl;

		
		//disk_entries = current_level->runs_list[0]->entries;
		// Insert entry to Result
		disk_bPTree = levels[i].runs_list[0]->bPTree;		
		bpt_IO_Check += disk_bPTree->bpt_search(Lower, Upper, bpt_IO_Check);


		//IO_Check = IO_Check + int(levels[i].runs_list[0]->entries.size() / DEFAULT_BUFFER_NUM_PAGES);    // 다음레벨에 있으면 한번 보고(+1) 레벨에 따라 2배수 더해줌(+{다음레벨 들어있는 양/버퍼사이즈}) 몫
		//if (levels[i].runs_list[0]->entries.size() % DEFAULT_BUFFER_NUM_PAGES > 0) {						// {다음레벨 들어있는 양/버퍼사이즈} 해준게 딱 나눠 떨어지지 않기 때문에,
		//	IO_Check = IO_Check + 1;																	// 자투리에 조금이라도 남아있을 수 있어서 +1해줌
		//}

		cout << "-------------------------------------------------------------------------------------" << endl;
		//current_level += 1;
	}
	cout << " * root to leaf nonono~ " << endl;
	cout << " * I/O Check = " << to_string(bpt_IO_Check) << endl;

}