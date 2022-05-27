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
void BPTree::bpt_insert(KEY_t key, FILE* filePtr) {  //in Leaf Node
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
			cursor->ptr2TreeOrData.dataPtr.push_back(filePtr);

			if (i != cursor->keys.size() - 1) {
				for (int j = cursor->keys.size() - 1; j > i; j--) {  // shifting the position for keys and datapointer
					cursor->keys[j] = cursor->keys[j - 1];
					cursor->ptr2TreeOrData.dataPtr[j] = cursor->ptr2TreeOrData.dataPtr[j - 1];
				}

				//since earlier step was just to inc. the size of vectors and making space, now we are simplying inserting
				cursor->keys[i] = key;
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
			vector<FILE*> virtualDataNode(cursor->ptr2TreeOrData.dataPtr);

			//finding the probable place to insert the key
			int i = std::upper_bound(cursor->keys.begin(), cursor->keys.end(), key) - cursor->keys.begin();

			virtualNode.push_back(key);          // to create space
			virtualDataNode.push_back(filePtr);  // to create space

			if (i != virtualNode.size() - 1) {
				for (int j = virtualNode.size() - 1; j > i; j--) {  // shifting the position for keys and datapointer
					virtualNode[j] = virtualNode[j - 1];
					virtualDataNode[j] = virtualDataNode[j - 1];
				}

				//inserting
				virtualNode[i] = key;
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

			//resizing and copying the keys & dataPtr to OldNode
			cursor->keys.resize((maxLeafNodeLimit) / 2 + 1);//check +1 or not while partitioning
			cursor->ptr2TreeOrData.dataPtr.reserve((maxLeafNodeLimit) / 2 + 1);
			for (int i = 0; i <= (maxLeafNodeLimit) / 2; i++) {
				cursor->keys[i] = virtualNode[i];
				cursor->ptr2TreeOrData.dataPtr[i] = virtualDataNode[i];
			}

			//Pushing new keys & dataPtr to NewNode
			for (int i = (maxLeafNodeLimit) / 2 + 1; i < virtualNode.size(); i++) {
				newLeaf->keys.push_back(virtualNode[i]);
				newLeaf->ptr2TreeOrData.dataPtr.push_back(virtualDataNode[i]);
			}

			if (cursor == root) {
				/*
					If cursor is root node we create new node
				*/

				Node* newRoot = new Node;
				newRoot->keys.push_back(newLeaf->keys[0]);
				new (&newRoot->ptr2TreeOrData.ptr2Tree) std::vector<Node*>;
				newRoot->ptr2TreeOrData.ptr2Tree.push_back(cursor);
				newRoot->ptr2TreeOrData.ptr2Tree.push_back(newLeaf);
				root = newRoot;
				//cout << "Created new Root!" << endl;  //----------------
			}
			else {
				// Insert new key in the parent
				insertInternal(newLeaf->keys[0], &parent, &newLeaf);
			}
		}
	}
}

void BPTree::insertInternal(KEY_t x, Node** cursor, Node** child) {  //in Internal Nodes
	if ((*cursor)->keys.size() < maxIntChildLimit - 1) {
		/*
			If cursor is not full find the position for the new key.
		*/
		int i = std::upper_bound((*cursor)->keys.begin(), (*cursor)->keys.end(), x) - (*cursor)->keys.begin();
		(*cursor)->keys.push_back(x);
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
			(*cursor)->ptr2TreeOrData.ptr2Tree[i + 1] = *child;
		}
		//cout << to_string(x) << ": Inserted key in the internal node :)" << endl;
	}
	else {  //splitting
	 //cout << "Inserted Node in internal node successful" << endl;
	 //cout << "Overflow in internal:( HAIYAA! splitting internal nodes" << endl;

		vector<KEY_t> virtualKeyNode((*cursor)->keys);
		vector<Node*> virtualTreePtrNode((*cursor)->ptr2TreeOrData.ptr2Tree);

		int i = std::upper_bound((*cursor)->keys.begin(), (*cursor)->keys.end(), x) - (*cursor)->keys.begin();  //finding the position for x
		virtualKeyNode.push_back(x);                                                                   // to create space
		virtualTreePtrNode.push_back(*child);                                                           // to create space

		if (i != virtualKeyNode.size() - 1) {
			for (int j = virtualKeyNode.size() - 1; j > i; j--) {  // shifting the position for keys and datapointer
				virtualKeyNode[j] = virtualKeyNode[j - 1];
			}

			for (int j = virtualTreePtrNode.size() - 1; j > (i + 1); j--) {
				virtualTreePtrNode[j] = virtualTreePtrNode[j - 1];
			}

			virtualKeyNode[i] = x;
			virtualTreePtrNode[i + 1] = *child;
		}

		KEY_t partitionKey;                                            //exclude middle element while splitting
		partitionKey = virtualKeyNode[(virtualKeyNode.size() / 2)];  //right biased
		int partitionIdx = (virtualKeyNode.size() / 2);

		//resizing and copying the keys & TreePtr to OldNode
		(*cursor)->keys.resize(partitionIdx);
		(*cursor)->ptr2TreeOrData.ptr2Tree.resize(partitionIdx + 1);
		(*cursor)->ptr2TreeOrData.ptr2Tree.reserve(partitionIdx + 1);
		for (int i = 0; i < partitionIdx; i++) {
			(*cursor)->keys[i] = virtualKeyNode[i];
		}

		for (int i = 0; i < partitionIdx + 1; i++) {
			(*cursor)->ptr2TreeOrData.ptr2Tree[i] = virtualTreePtrNode[i];
		}

		Node* newInternalNode = new Node;
		new (&newInternalNode->ptr2TreeOrData.ptr2Tree) std::vector<Node*>;
		//Pushing new keys & TreePtr to NewNode

		for (int i = partitionIdx + 1; i < virtualKeyNode.size(); i++) {
			newInternalNode->keys.push_back(virtualKeyNode[i]);
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
			insertInternal(partitionKey, findParent(root, *cursor), &newInternalNode);
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
				cursor = cursor->ptr2next;
				bpt_IO_Check = bpt_IO_Check + 1;
			}

			if (cursor->keys[idx] >= Lower && cursor->keys[idx] <= Upper) {
				cout << "* Find idx : " << to_string(cursor->keys[idx]) << endl;  //오래걸려서 주석처리함
			}
			else if (cursor->keys[idx] > Upper) {
				break;
			}

			idx++;

		}		

		//if (cursor->keys[idx] != Lower) {
		//	cout << "Leaf NOT FOUND~" << endl;

		//	////////////// Display.cpp 에 있는거
		//	Node* firstLeft = firstLeftNode(cursor);

		//	if (firstLeft == NULL) {
		//		cout << "No Data in the Database yet!" << endl;
		//		return;
		//	}
		//	while (firstLeft != NULL) {
		//		for (int i = 0; i < firstLeft->keys.size(); i++) {
		//			cout << firstLeft->keys[i] << " || ";
		//		}
		//		cout << "\n" << endl;
		//		firstLeft = firstLeft->ptr2next;
		//	}
		//	cout << endl;
		//	/////////////////


		//	return;
		//}
		//else {
		//	cout << "result = " << idx << endl;
		//}

		
		///*
		//	We can fetch the data from the disc in main memory using data-ptr
		//	using cursor->dataPtr[idx]
		//*/

		//string fileName = "src/DBFiles/";
		//string data;
		//fileName += to_string(key) + ".txt";
		//FILE* filePtr = fopen(fileName.c_str(), "r");
		//cout << "Hurray!! Key FOUND" << endl;
		//cout << "Corresponding Tuple Data is: ";
		//char ch = fgetc(filePtr);
		//while (ch != EOF) {
		//	printf("%c", ch);
		//	ch = fgetc(filePtr);
		//}
		//fclose(filePtr);
		//cout << endl;
	}
	return bpt_IO_Check;
}