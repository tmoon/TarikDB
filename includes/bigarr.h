#ifndef BIGARR_H
#define BIGARR_H

#define MAX_COL_SIZE 10

bigarr * create_arr();
bigarr * resize(bigarr *col);
int insert_unsorted(bigarr *col, int num);
void insert_unsorted_fast(bigarr *col, int num);
int binary_search(bigarr *col, int num);
int insert_sorted(bigarr *col, int num);
void free_arr(bigarr * col);
#endif 