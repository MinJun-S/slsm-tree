#include <iostream>
#include <fstream>
#include <algorithm>
#include <vector>
#include<string>

#include "B+ Tree.h"

using namespace std;
/*

바꾸는중 -> 원본코드(실행되는것)은 real_B-Plus-Tree22 프로젝트임

*/
void BPTree::bpt_insert(KEY_t key, FILE* filePtr, entry_t input_entry) {  //in Leaf Node
	/*
		1. If the node has an empty space, insert the key/reference pair into the node.
		2. If the node is already full, split it into two nodes, distributing the keys
		evenly between the two nodes. If the node is a leaf, take a copy of the minimum
		value in the second of these two nodes and repeat this insertion algorithm to
		insert it into the parent node. If the node is a non-leaf, exclude the middle
		value during the split and repeat this insertion algorithm to insert this excluded
		value into the parent node.
	*/

	if (root == NULL) {
		root = new Node;
		root->isLeaf = true;
		root->keys.push_back(key);
		root->val.push_back(input_entry.val);
		root->MBR_LB.push_back(input_entry.MBR_LB);
		root->MBR_UB.push_back(input_entry.MBR_UB);
		root->poly_name.push_back(input_entry.poly_name);
		new (&root->ptr2TreeOrData.dataPtr) std::vector<FILE*>;
		//// now, root->ptr2TreeOrData.dataPtr is the active member of the union
		root->ptr2TreeOrData.dataPtr.push_back(filePtr);

		//cout << to_string(key) << ": I am a ROOT!!" << endl;
		return;
	}
	else {
		Node* cursor = root;
		Node* parent = NULL;
		//searching for the possible position for the given key by doing the same procedure we did in search
		while (cursor->isLeaf == false) {
			parent = cursor;
			int idx = std::upper_bound(cursor->keys.begin(), cursor->keys.end(), key) - cursor->keys.begin();
			cursor = cursor->ptr2TreeOrData.ptr2Tree[idx];
		}

		//now cursor is the leaf node in which we'll insert the new key
		if (cursor->keys.size() < maxLeafNodeLimit) {
			/*
				If current leaf Node is not FULL, find the correct position for the new key and insert!
			*/
			int i = std::upper_bound(cursor->keys.begin(), cursor->keys.end(), key) - cursor->keys.begin();
			cursor->keys.push_back(key);
			cursor->val.push_back(input_entry.val);
			cursor->MBR_LB.push_back(input_entry.MBR_LB);
			cursor->MBR_UB.push_back(input_entry.MBR_UB);
			cursor->poly_name.push_back(input_entry.poly_name);
			cursor->ptr2TreeOrData.dataPtr.push_back(filePtr);

			if (i != cursor->keys.size() - 1) {
				for (int j = cursor->keys.size() - 1; j > i; j--) {  // shifting the position for keys and datapointer
					cursor->keys[j] = cursor->keys[j - 1];
					cursor->ptr2TreeOrData.dataPtr[j] = cursor->ptr2TreeOrData.dataPtr[j - 1];
				}

				//since earlier step was just to inc. the size of vectors and making space, now we are simplying inserting
				cursor->keys[i] = key;
				cursor->val[i] = input_entry.val;
				cursor->MBR_LB[i] = input_entry.MBR_LB;
				cursor->MBR_UB[i] = input_entry.MBR_UB;
				cursor->poly_name[i] = input_entry.poly_name;

				cursor->ptr2TreeOrData.dataPtr[i] = filePtr;
			}
			//cout << "Inserted successfully: " << key << endl;
		}
		else {
			/*
				DAMN!! Node Overflowed :(
				HAIYYA! Splitting the Node .
			*/
			vector<KEY_t> virtualNode(cursor->keys);
			vector<VAL_t> virtualNode_val(cursor->val);
			vector<MBR_LB_t> virtualNode_lb(cursor->MBR_LB);
			vector<MBR_UB_t> virtualNode_ub(cursor->MBR_UB);
			vector<POLY_t> virtualNode_poly(cursor->poly_name);
			vector<FILE*> virtualDataNode(cursor->ptr2TreeOrData.dataPtr);

			//finding the probable place to insert the key
			int i = std::upper_bound(cursor->keys.begin(), cursor->keys.end(), key) - cursor->keys.begin();

			virtualNode.push_back(key);          // to create space
			virtualNode_val.push_back(input_entry.val);          // to create space			
			virtualNode_lb.push_back(input_entry.MBR_LB);          // to create space
			virtualNode_ub.push_back(input_entry.MBR_UB);          // to create space
			virtualNode_poly.push_back(input_entry.poly_name);          // to create space
			virtualDataNode.push_back(filePtr);  // to create space

			if (i != virtualNode.size() - 1) {
				for (int j = virtualNode.size() - 1; j > i; j--) {  // shifting the position for keys and datapointer
					virtualNode[j] = virtualNode[j - 1];
					virtualNode_val[j] = virtualNode_val[j - 1];
					virtualNode_lb[j] = virtualNode_lb[j - 1];
					virtualNode_ub[j] = virtualNode_ub[j - 1];
					virtualNode_poly[j] = virtualNode_poly[j - 1];
					virtualDataNode[j] = virtualDataNode[j - 1];
				}

				//inserting
				virtualNode[i] = key;
				virtualNode_val[i] = input_entry.val;
				virtualNode_lb[i] = input_entry.MBR_LB;
				virtualNode_ub[i] = input_entry.MBR_UB;
				virtualNode_poly[i] = input_entry.poly_name;
				virtualDataNode[i] = filePtr;
			}
			/*
				BAZINGA! I have the power to create new Leaf :)
			*/

			Node* newLeaf = new Node;
			newLeaf->isLeaf = true;
			new (&newLeaf->ptr2TreeOrData.dataPtr) std::vector<FILE*>;
			//// now, newLeaf->ptr2TreeOrData.ptr2Tree is the active member of the union

			//swapping the next ptr
			Node* temp = cursor->ptr2next;
			cursor->ptr2next = newLeaf;
			newLeaf->ptr2next = temp;

			newLeaf->ptr2prev = cursor;

			//resizing and copying the keys & dataPtr to OldNode
			cursor->keys.resize((maxLeafNodeLimit) / 2 + 1);//check +1 or not while partitioning
			cursor->ptr2TreeOrData.dataPtr.reserve((maxLeafNodeLimit) / 2 + 1);
			for (int i = 0; i <= (maxLeafNodeLimit) / 2; i++) {
				cursor->keys[i] = virtualNode[i];
				cursor->val[i] = virtualNode_val[i];
				cursor->MBR_LB[i] = virtualNode_lb[i];
				cursor->MBR_UB[i] = virtualNode_ub[i];
				cursor->poly_name[i] = virtualNode_poly[i];
				cursor->ptr2TreeOrData.dataPtr[i] = virtualDataNode[i];
			}

			//Pushing new keys & dataPtr to NewNode
			for (int i = (maxLeafNodeLimit) / 2 + 1; i < virtualNode.size(); i++) {
				newLeaf->keys.push_back(virtualNode[i]);
				newLeaf->val.push_back(virtualNode_val[i]);       
				newLeaf->MBR_LB.push_back(virtualNode_lb[i]);     
				newLeaf->MBR_UB.push_back(virtualNode_ub[i]);     
				newLeaf->poly_name.push_back(virtualNode_poly[i]);
				newLeaf->ptr2TreeOrData.dataPtr.push_back(virtualDataNode[i]);
			}

			if (cursor == root) {
				/*
					If cursor is root node we create new node
				*/

				Node* newRoot = new Node;
				newRoot->keys.push_back(newLeaf->keys[0]);
				newRoot->val.push_back(newLeaf->val[0]);
				newRoot->MBR_LB.push_back(newLeaf->MBR_LB[0]);
				newRoot->MBR_UB.push_back(newLeaf->MBR_UB[0]);
				newRoot->poly_name.push_back(newLeaf->poly_name[0]);
				new (&newRoot->ptr2TreeOrData.ptr2Tree) std::vector<Node*>;
				newRoot->ptr2TreeOrData.ptr2Tree.push_back(cursor);
				newRoot->ptr2TreeOrData.ptr2Tree.push_back(newLeaf);
				root = newRoot;
				//cout << "Created new Root!" << endl;  //----------------
			}
			else {
				// Insert new key in the parent
				
				insertInternal(newLeaf->keys[0], &parent, &newLeaf, newLeaf->val[0], newLeaf->MBR_LB[0], newLeaf->MBR_UB[0], newLeaf->poly_name[0]);
			}
		}
	}
}

void BPTree::insertInternal(KEY_t x, Node** cursor, Node** child, VAL_t val, MBR_LB_t MBR_LB, MBR_UB_t MBR_UB, POLY_t poly_name) {  //in Internal Nodes
	if ((*cursor)->keys.size() < maxIntChildLimit - 1) {
		/*
			If cursor is not full find the position for the new key.
		*/
		int i = std::upper_bound((*cursor)->keys.begin(), (*cursor)->keys.end(), x) - (*cursor)->keys.begin();
		(*cursor)->keys.push_back(x);
		(*cursor)->val.push_back(val);
		(*cursor)->MBR_LB.push_back(MBR_LB);
		(*cursor)->MBR_UB.push_back(MBR_UB);
		(*cursor)->poly_name.push_back(poly_name);

		//new (&(*cursor)->ptr2TreeOrData.ptr2Tree) std::vector<Node*>;
		//// now, root->ptr2TreeOrData.ptr2Tree is the active member of the union
		(*cursor)->ptr2TreeOrData.ptr2Tree.push_back(*child);

		if (i != (*cursor)->keys.size() - 1) {  // if there are more than one element
			// Different loops because size is different for both (i.e. diff of one)

			for (int j = (*cursor)->keys.size() - 1; j > i; j--) {  // shifting the position for keys and datapointer
				(*cursor)->keys[j] = (*cursor)->keys[j - 1];
			}

			for (int j = (*cursor)->ptr2TreeOrData.ptr2Tree.size() - 1; j > (i + 1); j--) {
				(*cursor)->ptr2TreeOrData.ptr2Tree[j] = (*cursor)->ptr2TreeOrData.ptr2Tree[j - 1];
			}

			(*cursor)->keys[i] = x;
			(*cursor)->val[i] = val;
			(*cursor)->MBR_LB[i] = MBR_LB;
			(*cursor)->MBR_UB[i] = MBR_UB;
			(*cursor)->poly_name[i] = poly_name;

			(*cursor)->ptr2TreeOrData.ptr2Tree[i + 1] = *child;
		}
		//cout << to_string(x) << ": Inserted key in the internal node :)" << endl;
	}
	else {  //splitting
	 //cout << "Inserted Node in internal node successful" << endl;
	 //cout << "Overflow in internal:( HAIYAA! splitting internal nodes" << endl;

		vector<KEY_t> virtualKeyNode((*cursor)->keys);
		vector<VAL_t> virtualNode_val((*cursor)->val);
		vector<MBR_LB_t> virtualNode_lb((*cursor)->MBR_LB);
		vector<MBR_UB_t> virtualNode_ub((*cursor)->MBR_UB);
		vector<POLY_t> virtualNode_poly((*cursor)->poly_name);

		vector<Node*> virtualTreePtrNode((*cursor)->ptr2TreeOrData.ptr2Tree);

		int i = std::upper_bound((*cursor)->keys.begin(), (*cursor)->keys.end(), x) - (*cursor)->keys.begin();  //finding the position for x
		virtualKeyNode.push_back(x);                                                                   // to create space
		virtualNode_val.push_back(val);          // to create space
		virtualNode_lb.push_back(MBR_LB);          // to create space
		virtualNode_ub.push_back(MBR_UB);          // to create space
		virtualNode_poly.push_back(poly_name);          // to create space


		virtualTreePtrNode.push_back(*child);                                                           // to create space

		if (i != virtualKeyNode.size() - 1) {
			for (int j = virtualKeyNode.size() - 1; j > i; j--) {  // shifting the position for keys and datapointer
				virtualKeyNode[j] = virtualKeyNode[j - 1];
				virtualNode_val[j] = virtualNode_val[j - 1];
				virtualNode_lb[j] = virtualNode_lb[j - 1];
				virtualNode_ub[j] = virtualNode_ub[j - 1];
				virtualNode_poly[j] = virtualNode_poly[j - 1];

			}

			for (int j = virtualTreePtrNode.size() - 1; j > (i + 1); j--) {
				virtualTreePtrNode[j] = virtualTreePtrNode[j - 1];
			}

			virtualKeyNode[i] = x;
			virtualNode_val[i] = val;
			virtualNode_lb[i] = MBR_LB;
			virtualNode_ub[i] = MBR_UB;
			virtualNode_poly[i] = poly_name;

			virtualTreePtrNode[i + 1] = *child;
		}

		KEY_t partitionKey;                                            //exclude middle element while splitting
		partitionKey = virtualKeyNode[(virtualKeyNode.size() / 2)];  //right biased
		int partitionIdx = (virtualKeyNode.size() / 2);

		VAL_t partitionval;                                            //exclude middle element while splitting
		partitionval = virtualNode_val[(virtualNode_val.size() / 2)];  //right biased
		MBR_LB_t partitionlb;                                            //exclude middle element while splitting
		partitionlb = virtualNode_lb[(virtualNode_lb.size() / 2)];  //right biased
		MBR_UB_t partitionub;                                            //exclude middle element while splitting
		partitionub = virtualNode_ub[(virtualNode_ub.size() / 2)];  //right biased
		POLY_t partitionpoly;                                            //exclude middle element while splitting
		partitionpoly = virtualNode_poly[(virtualNode_poly.size() / 2)];  //right biased

		//resizing and copying the keys & TreePtr to OldNode
		(*cursor)->keys.resize(partitionIdx);
		(*cursor)->val.resize(partitionIdx);
		(*cursor)->MBR_LB.resize(partitionIdx);
		(*cursor)->MBR_UB.resize(partitionIdx);
		(*cursor)->poly_name.resize(partitionIdx);

		(*cursor)->ptr2TreeOrData.ptr2Tree.resize(partitionIdx + 1);
		(*cursor)->ptr2TreeOrData.ptr2Tree.reserve(partitionIdx + 1);
		for (int i = 0; i < partitionIdx; i++) {
			(*cursor)->keys[i] = virtualKeyNode[i];
			(*cursor)->val[i] = virtualNode_val[i];
			(*cursor)->MBR_LB[i] = virtualNode_lb[i];
			(*cursor)->MBR_UB[i] = virtualNode_ub[i];
			(*cursor)->poly_name[i] = virtualNode_poly[i];

		}

		for (int i = 0; i < partitionIdx + 1; i++) {
			(*cursor)->ptr2TreeOrData.ptr2Tree[i] = virtualTreePtrNode[i];
		}

		Node* newInternalNode = new Node;
		new (&newInternalNode->ptr2TreeOrData.ptr2Tree) std::vector<Node*>;
		//Pushing new keys & TreePtr to NewNode

		for (int i = partitionIdx + 1; i < virtualKeyNode.size(); i++) {
			newInternalNode->keys.push_back(virtualKeyNode[i]);
			newInternalNode->val.push_back(virtualNode_val[i]);
			newInternalNode->MBR_LB.push_back(virtualNode_lb[i]);
			newInternalNode->MBR_UB.push_back(virtualNode_ub[i]);
			newInternalNode->poly_name.push_back(virtualNode_poly[i]);
		}

		for (int i = partitionIdx + 1; i < virtualTreePtrNode.size(); i++) {  // because only key is excluded not the pointer
			newInternalNode->ptr2TreeOrData.ptr2Tree.push_back(virtualTreePtrNode[i]);
		}

		if ((*cursor) == root) {
			/*
				If cursor is a root we create a new Node
			*/
			Node* newRoot = new Node;
			newRoot->keys.push_back(partitionKey);
			newRoot->val .push_back(partitionval);
			newRoot->MBR_LB.push_back(partitionlb);
			newRoot->MBR_UB.push_back(partitionub);
			newRoot->poly_name.push_back(partitionpoly);

			new (&newRoot->ptr2TreeOrData.ptr2Tree) std::vector<Node*>;
			newRoot->ptr2TreeOrData.ptr2Tree.push_back(*cursor);
			//// now, newRoot->ptr2TreeOrData.ptr2Tree is the active member of the union
			newRoot->ptr2TreeOrData.ptr2Tree.push_back(newInternalNode);

			root = newRoot;
			//cout << " Created new ROOT!" << endl;  //------------------
		}
		else {
			/*
				::Recursion::
			*/
			insertInternal(partitionKey, findParent(root, *cursor), &newInternalNode, partitionval, partitionlb, partitionub, partitionpoly);
		}
	}
}

int BPTree::bpt_search(KEY_t Lower, KEY_t Upper, int bpt_IO_Check) {
	/* B+tree 하나에 대한 Range query */
	if (root == NULL) {
		cout << "Data Inserted yet" << endl;
		return bpt_IO_Check;
	}
	else {
		Node* cursor = root;
		vector<KEY_t>::iterator end_of_node;
		// 루트부터 리프까지 타고 내려감
		while (cursor->isLeaf == false) {
			/*
				upper_bound returns an iterator pointing to the first element in the range
				[first,last) which has a value greater than �val�.(Because we are storing the
				same value in the right node;(STL is doing Binary search at back end)
			*/
			int idx = std::upper_bound(cursor->keys.begin(), cursor->keys.end(), Lower) - cursor->keys.begin();
			cursor = cursor->ptr2TreeOrData.ptr2Tree[idx];  //upper_bound takes care of all the edge cases
			bpt_IO_Check = bpt_IO_Check + 1;
			
		}
		//리프 노드 하나에서 Lower key값에 가장 가까운(=/=똑같은) 값을 찾음
		int idx = std::lower_bound(cursor->keys.begin(), cursor->keys.end(), Lower) - cursor->keys.begin();  //Binary search
		//cout << "internal idx : " << idx << endl;

		//bpt_IO_Check = bpt_IO_Check + 1;  // 루트부터 안갈 경우 이걸 추가해야함(=시작 리프노드 IO)
		// Lower key부터 Upper key까지 범위 안에 들어오는거 리프에서만 쭉 이동하면서 다 가져욤 (Lower <= x <= Upper)
		while (1) {
			
			end_of_node = cursor->keys.end();
			if (cursor->keys[idx - 1] == (*end_of_node)) {
				if (cursor->ptr2next == NULL) {
					break;
				}
				cursor = cursor->ptr2next;
				bpt_IO_Check = bpt_IO_Check + 1;
				idx = 0;
			}

			/*if (cursor->keys.size() <= idx) {
				break;
			}*/

			if (cursor->keys[idx] >= Lower && cursor->keys[idx] <= Upper) {
				/* 찾았다! -> 찾은거 출력 */
				// 이때, 중복되는 poly name은 제거하고 출력한다 (정렬은 이미 되어있음)   <여기서 seg fault 뜰수도 있음 (-> 트리 전체에서 가장 맨 앞에 있는 값이면 idx -1이 없어서)>
				if (cursor->poly_name[idx] != cursor->poly_name[idx - 1]) {
					//cout << "* Find geohash : " << to_string(cursor->keys[idx]) << endl;  //오래걸려서 주석처리함
					printf("* Key : %d  ||  Poly_name : %d  ||  LB : (%f, %f)  ||  UB : (%f, %f)\n", cursor->keys[idx], cursor->poly_name[idx], cursor->MBR_LB[idx].x, cursor->MBR_LB[idx].y, cursor->MBR_UB[idx].x, cursor->MBR_UB[idx].y);
				}				
			}
			else if (cursor->keys[idx] > Upper) {
				cout << "component done (Reach to end)" << endl;
				break;
			}

			idx++;

		}		
	}
	return bpt_IO_Check;
}


int BPTree::bpt_knn(KEY_t query, int distance, int bpt_IO_Check, int k) {
	bpt_IO_Check = 0;
	/* B+tree 하나에 대한 Range query */
	if (root == NULL) {
		cout << "Data Inserted yet" << endl;
		return bpt_IO_Check;
	}
	else {
		Node* cursor = root;
		vector<KEY_t>::iterator end_of_node;
		vector<KEY_t>::iterator start_of_node;
		// 루트부터 리프까지 타고 내려감
		while (cursor->isLeaf == false) {
			/*
				upper_bound returns an iterator pointing to the first element in the range
				[first,last) which has a value greater than �val�.(Because we are storing the
				same value in the right node;(STL is doing Binary search at back end)
			*/
			int idx = std::upper_bound(cursor->keys.begin(), cursor->keys.end(), query) - cursor->keys.begin();
			cursor = cursor->ptr2TreeOrData.ptr2Tree[idx];  //upper_bound takes care of all the edge cases
			bpt_IO_Check = bpt_IO_Check + 1;

		}
		//리프 노드 하나에서 Lower key값에 가장 가까운(=/=똑같은) 값을 찾음
		int idx = std::lower_bound(cursor->keys.begin(), cursor->keys.end(), query) - cursor->keys.begin();  //Binary search
		//cout << "internal idx : " << idx << endl;

		int front = idx;
		int back = idx;
		Node* f_cursor = cursor;
		Node* b_cursor = cursor;
		k = 2 * k;
		
		//bpt_IO_Check = bpt_IO_Check + 1;  // 루트부터 안갈 경우 이걸 추가해야함(=시작 리프노드 IO)
		// Lower key부터 Upper key까지 범위 안에 들어오는거 리프에서만 쭉 이동하면서 다 가져욤 (Lower <= x <= Upper)
		while (k > 0) {
			end_of_node = b_cursor->keys.end();
			start_of_node = f_cursor->keys.begin();
			if (b_cursor->keys[back - 1] == (*end_of_node)) {
				if (b_cursor->ptr2next == NULL) {
					if (f_cursor->ptr2prev == NULL) {
						break;
					}
				}
				else {
					b_cursor = b_cursor->ptr2next;
					bpt_IO_Check = bpt_IO_Check + 1;
					back = 0;
				}
			}
			else {
				back++;
				k--;
			}
			
			if (f_cursor->keys[front + 1] == (*start_of_node)) {
				if (f_cursor->ptr2prev == NULL) {
					if (b_cursor->ptr2next == NULL) {
						break;
					}
				}
				else {
					f_cursor = f_cursor->ptr2prev;
					bpt_IO_Check = bpt_IO_Check + 1;
					front = f_cursor->keys.size() - 1;
				}
			}
			else {
				front--;
				k--;
			}
		}
	}
	return bpt_IO_Check;
}