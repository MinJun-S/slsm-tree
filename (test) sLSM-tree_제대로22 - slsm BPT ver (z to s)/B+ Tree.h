#include <iostream>
#include <vector>
#include <algorithm>
#include <fstream>
#include <string>
#include "types.h"
//#include <filesystem>
using namespace std;
/*

바꾸는중 -> 원본코드(실행되는것)은 real_B-Plus-Tree22 프로젝트임

*/
#ifndef NODE_H
#define NODE_H

class Node {
	/*
		Generally size of the this node should be equal to the block size. Which will limit the number of disk access and increase the accesssing time.
		Intermediate nodes only hold the Tree pointers which is of considerably small size(so they can hold more Tree pointers) and only Leaf nodes hold
		the data pointer directly to the disc.

		IMPORTANT := All the data has to be present in the leaf node
	*/
public:
	bool isLeaf;
	vector<KEY_t> keys;
	//Node* ptr2parent; //Pointer to go to parent node CANNOT USE check https://stackoverflow.com/questions/57831014/why-we-are-not-saving-the-parent-pointer-in-b-tree-for-easy-upward-traversal-in
	Node* ptr2next;              //Pointer to connect next node for leaf nodes
	Node* ptr2prev;              //Pointer to connect prev node for leaf nodes
	union ptr {                  //to make memory efficient Node
		vector<Node*> ptr2Tree;  //Array of pointers to Children sub-trees for intermediate Nodes
		vector<FILE*> dataPtr;   // Data-Pointer for the leaf node

		ptr();   // To remove the error !?
		~ptr();  // To remove the error !?
	} ptr2TreeOrData;

	friend class BPTree;  // to access private members of the Node and hold the encapsulation concept
public:
	Node();
};

class BPTree {
	/*
		::For Root Node :=
			The root node has, at least two tree pointers
		::For Internal Nodes:=
			1. ceil(maxIntChildLimit/2)     <=  #of children <= maxIntChildLimit
			2. ceil(maxIntChildLimit/2)-1  <=  #of keys     <= maxIntChildLimit -1
		::For Leaf Nodes :=
			1. ceil(maxLeafNodeLimit/2)   <=  #of keys     <= maxLeafNodeLimit -1
	*/
private:
	int maxIntChildLimit;                                   //Limiting  #of children for internal Nodes!
	int maxLeafNodeLimit;                                   // Limiting #of nodes for leaf Nodes!!!
	Node* root;                                             //Pointer to the B+ Tree root
	void insertInternal(KEY_t x, Node** cursor, Node** child);  //Insert x from child in cursor(parent)
	Node** findParent(Node* cursor, Node* child);
	Node* firstLeftNode(Node* cursor);

public:
	BPTree(void);
	~BPTree(void);
	BPTree(int degreeInternal, int degreeLeaf);
	Node* getRoot();
	int getMaxIntChildLimit();
	int getMaxLeafNodeLimit();
	void setRoot(Node *);
	//void display(Node* cursor);
	//void seqDisplay(Node* cursor);
	int bpt_search(KEY_t Lower, KEY_t Upper, int bpt_IO_Check);
	void bpt_insert(KEY_t key, FILE* filePtr);
	/* void removeKey(int key);
	 void removeInternal(int x, Node* cursor, Node* child);*/
};

#endif