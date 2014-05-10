#ifndef BPLUSTREE_H
#define BPLUSTREE_H


// search
int binary_search_leaf(node *c, int num);
node * find_leaf( node * root, int key);
record * find( node * root, int key);
int split_at( int length );


/////////////////////// Insert functions //////////////////////////////

// prepare node and records
record * make_record(int value);
node * make_node( void );
node * make_leaf( void );

// other insertion routines
int get_left_index(node * parent, node * left);
node * insert_into_leaf( node * leaf, int key, record * pointer );
void insert_record(record * base, record * ptr);
node * insert_into_leaf_after_splitting(node * root, node * leaf, int key, record * pointer);
node * insert_into_node(node * root, node * parent, 
		int left_index, int key, node * right);
node * insert_into_node_after_splitting(node * root, node * parent, int left_index, 
		int key, node * right);
node * insert_into_parent(node * root, node * left, int key, node * right);
node * insert_into_new_root(node * left, int key, node * right);
node * start_new_tree(int key, record * pointer);
node * insert( node * root, int key, int value );


#endif // BPLUSTREE_H