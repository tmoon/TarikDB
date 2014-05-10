#ifndef ALL_STRUCTS_H
#define ALL_STRUCTS_H

#define ORDER 4097

typedef struct 
{
  int size;
  int max_size;
  int *arr;
}bigarr;


typedef struct 
{
	char *name;
	int type; //0 = btree, 1= bigarr, sorted, 2=bigarr, unsorted, 3 = binay table (normal), 4 = binary table (b+tree)
	void * object; // pointer to array or tree structure // binary table: val array/ B+tree
  bigarr * sec_obj; //(optional) unsorted array if b+tree // binary table: positions, 
}col_data;

// currently our key and values are the same
// but when we'll implement db that resides in disk
// we can change this to file pointers
typedef struct record {
	int value;
  struct record * next;
} record;

typedef struct node {
	void ** pointers;
	int * keys;
	struct node * parent;
	bool is_leaf;
	int num_keys;
	struct node * next; // Used for queue.
} node;

typedef struct 
{
  int type; 
  /*0 = select whole col, 
  1 = select 1 elem, 
  2 = select range,
  3 = fetch,
  4 = create,
  5 = load,
  6 = insert
  */ 
  int num_args; //number of args
  char **args; //max input args
}command;


#endif