#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h> /* memset */

#include "all_structs.h"
#include "bigarr.h"

bigarr * create_arr()
{
	bigarr * col;
	col = malloc(sizeof(bigarr));
	col->size = 0;
	col->max_size = MAX_COL_SIZE;

	col->arr = malloc(MAX_COL_SIZE * sizeof(int));
	return col;
}

bigarr * resize(bigarr *col)
{	

	int  *temp = realloc(col->arr, 2 * col->max_size * sizeof(int));
	if (temp != NULL)
		col->arr = temp;
	else
		free(temp);
	col->max_size *= 2;

	return col;
}

int insert_unsorted(bigarr *col, int num)
{
	if (col -> size == col -> max_size)
		col = resize(col);

	col->arr[col->size] = num;
	col->size++;

	return 0;	
}

void insert_unsorted_fast(bigarr *col, int num)
{
	col->arr[col->size++] = num;
}

// find the index of the smallest number >= num
int binary_search(bigarr *col, int num)
{

	int imin =0;
	int imax = col->size-1;
	int imid=0;

	if (col->arr[imax] < num)
		return -1;

	while(imax >= imin)
	{
		imid = (imin + imax)/2;
		if (col->arr[imid] == num)
			return imid;
		else if (col->arr[imid] < num)
			imin = imid + 1;
		else if (col->arr[imid] > num)
			imax = imid - 1;
	}

	return imid;
}
int insert_sorted(bigarr *col, int num)
{
	// printf("%d\n", col->size);
	if (col -> size == col -> max_size)
		resize(col);

	int ind = -1;

	ind = binary_search(col, num);

	if (ind != -1)
	{
		memcpy(&col->arr[ind+1], &col->arr[ind],  (col->size - ind) * sizeof(int));
		col->arr[ind] = num;
	}
	else
		col->arr[col->size] = num;

	col->size++;

	return 0;	
}

void free_arr(bigarr * col)
{
	free(col->arr);
	free(col);
}