/*
	실험용 basic LSM 코드 수정본
*/
#include <iostream>

#include "buffer.h"

using namespace std;

VAL_t * Buffer::get(KEY_t key) const {
    entry_t search_entry;
    set<entry_t>::iterator entry;
    VAL_t *val;

    search_entry.key = key;
    entry = entries.find(search_entry);

    if (entry == entries.end()) {
        return nullptr;
    } else {
        val = new VAL_t;
        *val = entry->val;
        return val;
    }
}

vector<entry_t> * Buffer::range(KEY_t start, KEY_t end) const {
    entry_t search_entry;
    set<entry_t>::iterator subrange_start, subrange_end;

    search_entry.key = start;
    subrange_start = entries.lower_bound(search_entry);

    search_entry.key = end;
    subrange_end = entries.upper_bound(search_entry);

    return new vector<entry_t>(subrange_start, subrange_end);
}

//bool Buffer::put(KEY_t key, VAL_t val, BPTree** bPTree) {
bool Buffer::put(KEY_t key, VAL_t val) {
    entry_t entry;
    set<entry_t>::iterator it;
    bool found;

    if (entries.size() == max_size) {
        return false;
    } else {
        entry.key = key;
        entry.val = val;

        tie(it, found) = entries.insert(entry);

		//insertionMethod(bPTree, key);

        // Update the entry if it already exists
        if (found == false) {
            entries.erase(it);
            entries.insert(entry);
        }

        return true;
    }
}

void Buffer::empty(void) {
    entries.clear();
}

void Buffer::insertionMethod(BPTree** bPTree, KEY_t key) {
	// 노드 생성 -> BPT의 키값(geohash) = rollNo
	// 'p'커맨드처럼 하나씩 입력하는거니까 -> 이 함수를 LSM트리에서 entries.put()대신에 사용하고, 파일 읽기만 바꿔주면 될듯
	// set<entry> entries는 set<>버리고 BPT로 바꿔야하나
	
	string fileName = "src/DBFiles/";
	fileName += "1.txt";
	//fileName += to_string(key) + ".txt";
	FILE* filePtr = fopen(fileName.c_str(), "w");
	string userTuple = to_string(key) + "\n";
	fprintf(filePtr, userTuple.c_str());
	(*bPTree)->bpt_insert(key, filePtr);
	//bPTree->bpt_insert(key, filePtr);
	fclose(filePtr);
	
}