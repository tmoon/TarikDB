#ifndef WORKER_H
#define WORKER_H

void *thread_handler(void *serverid) ;
char * get_file(int flag, int s2);
//parser 
command * parse_cmd(char * line);
// helpers
int find_index_db(char * col_name);
int find_index_temp(char * col_name);
void set_temp_vars(int num);
void cache_set(command *c);
void save_col(col_data * col);
void close_server();
int put_in_temp(char * name, bigarr * col);
int compare (const void * a, const void * b);

// execution functions
int select_col(command * c);
int select_elem(command * c);
int select_range(command * c);
int fetch1(command * c, int s);
int fetch2(command * c, int s);
int fetch3(command * c, int s);
int create_col(command * c);
int load_db(command *c, int s);
int insert_elem(command * c);
int print_tuples(command *c, int s);

//math and aggregrations
int find_min(int a, int b);

void add_col(command * c);
void sub_col(command * c);
void mul_col(command * c);
void div_col(command * c);

void min_col(command * c);
void max_col(command * c);
void sum_col(command * c);
void avg_col(command * c);
void count_col(command * c);

// updates
void update_col(command * c);
void insert_column(char *col_name, int v);
void insert_master(command *c);
void delete_col(bigarr * posArr, char * col_name);
void delete_master(command * c);

// master execution function
void execute_cmd(command * c, int s);

// joins
void loop_join(char * v1, char * v2);
void tree_join(char * v1, char * v2) ;
int hash_function(int i);
int binary_search_bucket_left(bigarr *col, int num);
void hash_join(char * v1, char * v2) ;
bigarr * sort_join_helper(bigarr * vids, bigarr * col);
void sort_join(char* v1, char* v2) ;

#endif