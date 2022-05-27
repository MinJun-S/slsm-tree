/*
	����� basic LSM �ڵ� ������
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
	// ��� ���� -> BPT�� Ű��(geohash) = rollNo
	// 'p'Ŀ�ǵ�ó�� �ϳ��� �Է��ϴ°Ŵϱ� -> �� �Լ��� LSMƮ������ entries.put()��ſ� ����ϰ�, ���� �б⸸ �ٲ��ָ� �ɵ�
	// set<entry> entries�� set<>������ BPT�� �ٲ���ϳ�
	
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