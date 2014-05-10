/*
CS 165: Tarik Adnan Moon

This file has all the worker functions and thread handers in it.


*/


#define _GNU_SOURCE
#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <stdbool.h>
#include <unistd.h>


#include "all_structs.h"
#include "bplustree.h"
#include "bigarr.h"
#include "worker.h"

#define MAX_COLS 10000 //sopports max 10k columns
#define BUFF_MAX_SIZE 524288 //512KB Buffer

#define DEBUG true

#define PAGE_SZ 16384 // 16KB 

char str[BUFF_MAX_SIZE];

//keep a global array of 10000 column pointer
col_data *db_cols[MAX_COLS];
int db_num_cols=0;

// destroy the selectors after we are done with client cmds
//keep a global array of temp column names
char ** temp_col_names = NULL;

//keep a global array of the arrays pointed by those temp names
bigarr ** temp_cols = NULL;
int num_temp_cols=0;
int temp_var_count = 0;

// because we are just doing one join at a time, we keep
// the join lazy fetch as global
col_data * join_col1 = NULL;
col_data * join_col2 = NULL;

/* This is the routine that the threads execute */
void *thread_handler(void *serverid) {

	
    int *myarg;
    myarg = (int *)serverid;
    int s2 = *myarg;
    if (DEBUG)
	    printf("Connected to client %d\n",s2 );

	char *fname;
	bool got_command = false;

	// BUFFER: get a command file and write to disk as temp_command.
	// to make sure we don't lose anything
	fname = get_file(0,s2);
	
	if (DEBUG)
		printf("GOT CMD FILE\n");
	got_command = true;


	if (got_command)
	{
	    FILE * fp;
	    fp = fopen(fname,"r");

	    char * line = NULL;
	    size_t len = 0;
	    ssize_t read;

	    if (fp == NULL)
	        exit(EXIT_FAILURE);

	    while ((read = getline(&line, &len, fp)) != -1) 
	    {
	        command * c = parse_cmd(line);

	        if (c->type != -1)
		        execute_cmd(c, s2);
		    
		    free(c);
	    }
	    // free(line);
	}
	unlink(fname);
	send(s2, "EXIT", 4, 0);
 	close(s2);
 	 printf("CONNECTION CLOSED\n");
  	
  	//TODO: free all memory and send closed signal

  	pthread_exit(NULL);

}

/*Gets file from the client and writes in temp folder for postprocessing*/
char * get_file(int flag, int s2)
{
    FILE * fp;
    size_t CHARSIZE = sizeof(char);
    char * filename = malloc(64* CHARSIZE); // free after done

    int n, i=0, temp_var_count=0;

    if (flag ==0) //get command file
    {
        
        strcpy(filename, "./temp/temp_cmd_");
        sprintf(filename+16,"%d.cmd",s2);

        fp = fopen(filename, "w");    
    }
    else //get data file(s)
    {
        strcpy(filename, "./temp/data_table_");
        sprintf(filename+18,"%d.data",s2);

        fp = fopen(filename, "w");  
    }

    // get the whole file by buffering and then write to disk
    bool done = false;
    do
    {
        
        n = recv(s2, str, BUFF_MAX_SIZE, 0);
        // if command file we find the number of temp variables we need to store
     
            for (i = 0; i < n; ++i)
            {   if (flag == 0)
                {
                    if (str[i] == '=')
                        temp_var_count++;
                }

                if (str[i] == '|')
                {
                    done = true;
                    // printf("I,N %d %d\n",i,n );
                    n--;
                }
                    
            } 

        fwrite(str,CHARSIZE,n,fp);
        if (n <=0)
            done = true;
    }
    while(!done);

    if (flag == 0) // if command file 
         set_temp_vars(temp_var_count);
     printf("TMP VARS%d\n", temp_var_count);

    fclose(fp);

    return filename;
}

//parser 
command * parse_cmd(char * line)
{
	printf("%s",line );
  command * cmd = malloc(sizeof(command));
  int num_input = 0, input_count=4; //allocating slightly more memory for later use
  int type = -1,i=0;
  char * pch;
  char ch;


  while ((ch=line[i])!=')')
  {
    i++;
    if (ch == ',')
      input_count++; 
  }
  cmd->args = malloc(input_count * sizeof(char *));
 // if equal sign then take the intermediate var
  if (strchr(line,'=')!=NULL)
  {
    //then it is a select, fetch, join or math operations
    pch = strtok (line,"=");

    while(pch!=NULL)
    {
      cmd->args[num_input] = pch;
      if (num_input == 1)
      {
        if (strncmp(pch,"select", 6) == 0)
          type = 0;
        else if (strncmp(pch,"fetch", 5) == 0)
        {
        	if (strncmp ( cmd->args[0], "join", 4) == 0)
        		type = 9;
        	else//fetch
        		type = 8;
        }
        else if (strncmp(pch,"treejoin", 8) == 0)
        	type = 10;
        else if (strncmp(pch,"hashjoin", 8) == 0)
        	type = 11;
        else if (strncmp(pch,"loopjoin", 8) == 0)
        	type = 12;
        else if (strncmp(pch,"sortjoin", 8) == 0)
        	type = 13;
        //TODO: add add, update et here
        else if (strncmp(pch,"min", 3) == 0)
        	type = 14;
        else if (strncmp(pch,"max", 3) == 0)
        	type = 15;
        else if (strncmp(pch,"sum", 3) == 0)
        	type = 16;
        else if (strncmp(pch,"avg", 3) == 0)
        	type = 17;
        else if (strncmp(pch,"count", 4) == 0)
        	type = 18;
        else if (strncmp(pch,"add", 3) == 0)
        	type = 19;
        else if (strncmp(pch,"sub", 3) == 0)
        	type = 20;
        else if (strncmp(pch,"mul", 3) == 0)
        	type = 21;
        else if (strncmp(pch,"div", 3) == 0)
        	type = 22;
        else
          printf("Unknown function entered: %s\n", pch);
      }
      num_input++;
      pch = strtok  (NULL, "=(,)\n");
    }
   
    // figure out the select type
    if (type == 0)
    {
      if (num_input == 3)
        type = 0;
      else if (num_input == 4)
        type = 1;
      else if (num_input == 5)
        type = 2;
      else
        perror("Something wrong with num arg in select");
    }
  }
  else
  {
    pch = strtok (line,"(,)\n");
    while(pch!=NULL)
    {
      cmd->args[num_input] = pch;

      if (num_input == 0)
      {
        if (strncmp(pch,"fetch", 5) == 0)
          type = 3;
        else if (strncmp(pch,"create", 6) == 0)
          type = 4;
        else if (strncmp(pch,"load",4 ) == 0)
          type = 5;
        else if (strncmp(pch,"insert", 6) == 0)
        {
        	if (input_count > 3)
        		type = 23;
        	else
        		type = 6;
        }
        else if (strncmp(pch,"tuple",5)==0)
          type = 7;
     	else if (strncmp(pch,"delete",6)==0)
          type = 24;
        else if (strncmp(pch,"update",6)==0)
          type = 25;
        else if (strncmp(pch,"exit",4)==0)
          type = 27;
      	else
          printf("Unknown function entered: %s\n", pch);
      }

      num_input++;
      pch = strtok  (NULL, "(,)\n");
    }
  }


  cmd->num_args = num_input;

  cmd->type = type;

  // for (int i = 0; i < num_input; ++i)
  // 	printf("%s\n",cmd->args[i]);
  // printf("TYPE %d\n", cmd->type);
  return cmd;
}

// helpers to find the relevant columns
int find_index_db(char * col_name)
{
	for (int i = 0; i < db_num_cols; ++i)
	{
		if (strcmp(col_name,db_cols[i]->name)==0)
			return i;
	}
	printf("index not found %s", col_name);
	return -1;
}

int find_index_temp(char * col_name)
{

	for (int i = 0; i < num_temp_cols; ++i)
	{
		if (strcmp(col_name, temp_col_names[i])==0)
			return i;
	}
	// printf("index not found %s", col_name);
	return -1;
}

// set temp variables. Called each time a command file is called
void set_temp_vars(int num)
{
	if (temp_col_names!= NULL)
	{
		temp_var_count += num;
		char ** new_temp_col_names = realloc(temp_col_names, temp_var_count * sizeof(char*) );
		if (new_temp_col_names!=NULL)
			temp_col_names = new_temp_col_names;

		bigarr ** new_temp_cols = realloc(temp_cols, temp_var_count * sizeof(bigarr *) );
		if (new_temp_cols!=NULL)
			temp_cols= new_temp_cols;
	}
	else
	{
		temp_col_names = malloc(num * sizeof(char *));
		temp_cols = malloc(num * sizeof(bigarr *));
		temp_var_count = num;
	}

}

// dumps the columns to disk

void save_col(col_data * col)
{	
	char filepath[100];
	strcpy(filepath,"./db/");
	strcat(filepath, col->name);
	FILE *fp = fopen(filepath, "wb");

	if (col->type == 0) //b+tree
	{
		node * c = col->object;
		bigarr * pos = col->sec_obj;
		int * buffer = malloc(2 * pos->size * sizeof(int));
		
		if (c == NULL) 
			return;
		
		// go to the first leaf
		while (!c->is_leaf) 
			c = (node *)c->pointers[0];

		// now traverse
		int ind = 0, val=0;
		do
		{
			for (int i = 0; i < c->num_keys; ++i)
			{
				record * r = c->pointers[i];
				val = c->keys[i];
				// traverse the linked list
				do
				{
					buffer[ind++] = val;
					buffer[ind++] = r->value;
				}
				while( (r = r->next) != NULL);
			}
		}
		while((c = c->pointers[ORDER-1]) != NULL);
		
		fwrite(buffer, sizeof(int), 2 * pos->size, fp);
        fclose(fp);
	}
	else //dump the array
	{
		bigarr * data = col->object;
		fwrite(data->arr, sizeof(int), data->size, fp);
        fclose(fp);
	}
}

void close_server()
{
	for (int i = 0; i < db_num_cols; ++i)
		save_col(db_cols[i]);
	printf("SAVED ALL COLUMNS TO DISK. CLOSING THE SERVER.\n");
	exit(0);
}
// // set the caching behavior of the db
void cache_set(command *c)
{
	int i = 0;
}

int key_ind = 0;
bool need_sort = false;
// comparator function for qsort
int compare (const void * a, const void * b)
{
//keep key_ind global and change before use
  return ( *((int*)a + key_ind) - *((int*)b + key_ind) );
}


/**********************************************************************
 **************************** MATH OPERATIONS *************************
 **********************************************************************/
int STEP = PAGE_SZ / sizeof(int);
int find_min(int a, int b)
{
	if (a > b)
		return b;
	else
		return a;
}

void add_col(command * c) {
	int ind1= find_index_temp(c->args[2]);
	int ind2 = find_index_temp(c->args[3]);

	bigarr * col1 = temp_cols[ind1];
	bigarr * col2 = temp_cols[ind2];

    if (col1->size == col2-> size) 
    {
    	int * arr1 = col1->arr;
    	int * arr2= col2->arr;
        bigarr * res = create_arr();

	    // cache conscious
	    for (int i = 0; i < col1->size; i +=STEP/2)  
	    {
	    	for (int r = i; r < find_min(i + STEP/2, col1->size); ++r)  
	    		insert_unsorted(res, arr1[r] + arr2[r] );
	        
	    }
		put_in_temp(c->args[0], res);
	}

}

void sub_col(command * c) {
	int ind1= find_index_temp(c->args[2]);
	int ind2 = find_index_temp(c->args[3]);

	bigarr * col1 = temp_cols[ind1];
	bigarr * col2 = temp_cols[ind2];

    if (col1->size == col2-> size) 
    {
    	int * arr1 = col1->arr;
    	int * arr2= col2->arr;
        bigarr * res = create_arr();

	    // cache conscious
	    for (int i = 0; i < col1->size; i += STEP/2)  
	    {
	    	for (int r = i; r < find_min(i + STEP/2, col1->size); ++r)  
	    		insert_unsorted(res, arr1[r] - arr2[r] );
	        
	    }
		put_in_temp(c->args[0], res);
	}

}

void mul_col(command * c) {
	int ind1= find_index_temp(c->args[2]);
	int ind2 = find_index_temp(c->args[3]);

	bigarr * col1 = temp_cols[ind1];
	bigarr * col2 = temp_cols[ind2];

    if (col1->size == col2-> size) 
    {
    	int * arr1 = col1->arr;
    	int * arr2= col2->arr;
        bigarr * res = create_arr();

	    // cache conscious
	    for (int i = 0; i < col1->size; i += STEP/2)  
	    {
	    	for (int r = i; r < find_min(i + STEP/2, col1->size); ++r)  
	    		insert_unsorted(res, arr1[r] * arr2[r] );
	        
	    }
		put_in_temp(c->args[0], res);
	}

}

void div_col(command * c) {
	// printf("DIV %s\n",c->args[0] );
	int ind1= find_index_temp(c->args[2]);
	int ind2 = find_index_temp(c->args[3]);

	bigarr * col1 = temp_cols[ind1];
	bigarr * col2 = temp_cols[ind2];

    if (col1->size == col2-> size) 
    {
    	int * arr1 = col1->arr;
    	int * arr2= col2->arr;
        bigarr * res = create_arr();

	    // cache conscious
	    for (int i = 0; i < col1->size; i += STEP/2)  
	    {
	    	for (int r = i; r < find_min(i + STEP/2, col1->size); ++r)  
	    		insert_unsorted(res, arr1[r] / arr2[r] );
	        
	    }
		put_in_temp(c->args[0], res);
	}

}

/**********************************************************************
 **************************** AGGREGATING OPERATIONS ******************
 **********************************************************************/

void min_col(command * c) {
	int ind= find_index_temp(c->args[2]);

	bigarr * data = temp_cols[ind];

	int * col = data->arr;
	int min_val = col[0];
    bigarr * res = create_arr();

    // cache conscious
    for (int i = 0; i < data->size; i += STEP)  
    {
    	for (int r = i; r < find_min(i + STEP, data->size); ++r)  
    	{
			if (col[r] < min_val)
				min_val = col[r];
    	}         
    }
    insert_unsorted(res, min_val);
	put_in_temp(c->args[0], res);

}

void max_col(command * c) {
	int ind= find_index_temp(c->args[2]);

	bigarr * data = temp_cols[ind];

	int * col = data->arr;
	int max_val = col[0];
    bigarr * res = create_arr();

    // cache conscious
    for (int i = 0; i < data->size; i += STEP)  
    {
    	for (int r = i; r < find_min(i + STEP, data->size); ++r)  
    	{
			if (col[r] > max_val)
				max_val = col[r];
    	}         
    }
    insert_unsorted(res, max_val);
	put_in_temp(c->args[0], res);

}

void sum_col(command * c) {
	int ind= find_index_temp(c->args[2]);

	bigarr * data = temp_cols[ind];

	int * col = data->arr;
	int sum_cols = 0;
    bigarr * res = create_arr();

    // cache conscious
    for (int i = 0; i < data->size; i += STEP)  
    {
    	for (int r = i; r < find_min(i + STEP, data->size); ++r)  
   				sum_cols += col[r];        
    }

    insert_unsorted(res, sum_cols);
	put_in_temp(c->args[0], res);

}

void avg_col(command * c) {
	int ind= find_index_temp(c->args[2]);

	bigarr * data = temp_cols[ind];

	int * col = data->arr;
	int sum_cols = 0;
    bigarr * res = create_arr();

    // cache conscious
    for (int i = 0; i < data->size; i += STEP)  
    {
    	for (int r = i; r < find_min(i + STEP, data->size); ++r)  
   				sum_cols += col[r];        
    }

    insert_unsorted(res, sum_cols/ data->size);
	put_in_temp(c->args[0], res);
}

void count_col(command * c) {
	int ind= find_index_temp(c->args[2]);

	bigarr * data = temp_cols[ind];

    bigarr * res = create_arr();

    if (DEBUG)
	    printf("size %d\n", data->size );
    insert_unsorted(res, data->size);
	put_in_temp(c->args[0], res);

}

//////////////// worker functions //////////

/////////// Select operations//////////////

// this one selects just one column
int select_col(command * c) //done
{
	char * temp_name = c->args[0];
	char * col_name = c-> args[2];

	// printf("%d\n", strcmp(,"t2a"));
	int ind = find_index_db(col_name); 

	bigarr * resArr = create_arr();
	bigarr * col;

	if (ind == -1)
	{
		perror("column not found!");
		exit(-1);
	}

	if (db_cols[ind]->type == 0) //case: b+tee
		col = db_cols[ind]->sec_obj;
	else
		col = db_cols[ind]->object;

	for (int i = 0; i < col->size; ++i)
		insert_unsorted(resArr, i);

	// put inside temp array
	put_in_temp(temp_name, resArr);

	return 0;
}

int select_elem(command * c) //done
{
	c->num_args++;
	c->args[4] = c->args[3]; // will get error when free
	return select_range(c);
}

int select_range(command * c) //done
{
	int i=0;
	char * temp_name = c->args[0];
	char * col_name = c-> args[2];

	int left = atoi(c->args[3]);
	int right = atoi(c->args[4]);
	int ind = find_index_db(col_name);

	bigarr * resArr = create_arr();

	if (ind == -1)
	{
		perror("column not found!");
		return 1;
	}


	if (db_cols[ind]->type == 0) //case: b+tee
	{
		// find leaf
		node * leaf = find_leaf(db_cols[ind]->object, left);
		// int left_ind=0;

		//then traverse to write (put ind in resArr) as long as it satisfies
		while(leaf != NULL)
		{
			record * r;
			for (i = 0; i < leaf->num_keys; ++i)
			{
				if (leaf->keys[i] >= left && leaf->keys[i] <= right)
				{
					r = leaf->pointers[i];
					do
					{
						insert_unsorted(resArr,r->value);
					}while((r=r->next) != NULL);	
				} 					
			}
			leaf = leaf->pointers[ORDER-1];
		}
	}
	else if (db_cols[ind]->type == 0) //sorted
	{
		bigarr * col = (bigarr *) db_cols[ind]->object;
		//binary search to find left and right
		int left_ind = binary_search(col, left);
		int right_ind =  binary_search(col, right);
		//right ind fix 
		if (col->arr[right_ind] != right)
			right_ind--;

		for (i = left_ind; i < right_ind; ++i)
		{
			insert_unsorted(resArr,i);
		}
	}
	else
	{
		// go through the whole array (linear search)
		bigarr * col = (bigarr *) db_cols[ind]->object;
		for (int i = 0; i < col->size; ++i)
		{
			if (col->arr[i] >= left && col->arr[i] <= right)
			{
				insert_unsorted(resArr,i);
			}
				
		}
	}

	// put inside temp array
	put_in_temp(temp_name,resArr);

	return 0;
}

// fetch values and send to client: stream
int fetch1(command * c, int s)
{
	int ind_col = find_index_db(c->args[1]);
	int ind_tmp = find_index_temp(c->args[2]);


	bigarr *col;

	if (db_cols[ind_col]->type == 0) //case: b+tee
	{
		col = db_cols[ind_col]->sec_obj;
	}
	else //bigarr
	{
		col = db_cols[ind_col]->object;
	}
	int * numArr = col->arr;

	int * posArr = temp_cols[ind_tmp]->arr;
	int arrSize = temp_cols[ind_tmp]->size;

	char num_buf[13];
	int str_len = 0, ind =0, n=0;
	while(ind < arrSize)
	{
		str_len = 0;
		strcpy(str,"");
		while(str_len < BUFF_MAX_SIZE && ind < arrSize)
		{
			n = sprintf (num_buf, "%d\n", numArr[posArr[ind]]);
			strcat(str, num_buf);
			++ind;
			str_len += n;
		}
		if (send(s, str, str_len, 0) == -1) 
        {
            perror("send");
            exit(-1);
		}
	}

	return 0;
}

// fetch values and store in temp var
int fetch2(command *c, int s)
{
	//send and then free
	int ind_col = find_index_db(c->args[2]);
	int ind_tmp = find_index_temp(c->args[3]);
	
	bigarr *col;

	if (db_cols[ind_col]->type == 0) //case: b+tee
	{
		col = db_cols[ind_col]->sec_obj;
	}
	else //bigarr
	{
		col = db_cols[ind_col]->object;
	}
	int * posArr = temp_cols[ind_tmp]->arr;

	//put in a new temp arr
	bigarr * temp_fetch_arr = create_arr();

	int count = temp_cols[ind_tmp]->size;
	// printf("IND TMP%d %d \n", ind_tmp, count);
	int i=0;
	for (i = 0; i < count; ++i)
	{
		// printf("%d %d\n",i, temp_fetch_arr->max_size );
		// printf("%s\n", );
		insert_unsorted(temp_fetch_arr, col->arr[posArr[i]]); 
	}

	// put inside temp array
	put_in_temp(c->args[0], temp_fetch_arr);

	return 0;
}

int put_in_temp(char * name, bigarr * col)
{
	// put inside temp array
	int ind = find_index_temp(name) ;
	if (ind == -1)
	{
		size_t size_name = (strlen(name) +1) * sizeof(char);

		temp_col_names[num_temp_cols] = malloc(size_name);
		strcpy(temp_col_names[num_temp_cols], name);

		temp_cols[num_temp_cols] = col;
		num_temp_cols++;
	}
	else
	{
		bigarr * tmp = temp_cols[ind];
		free_arr(tmp);
		temp_cols[ind] = col;
	}
	

	return 0;
}
// fetches binary table from the structure for joins
int lazy_fetch(command *c, int s)
{
	printf("LAZY MASTER\n");
	//send and then free
	int ind_col = find_index_db(c->args[2]);
	int ind_tmp = find_index_temp(c->args[3]);
		// bigarr * col;

	col_data * join_col = malloc(sizeof(col_data));

	if (db_cols[ind_col]->type == 0) //case: b+tee
	{
		// col = db_cols[ind_col]->sec_obj;
		join_col->type = 4;
	}
	else //bigarr
	{
		// col = db_cols[ind_col]->object;
		join_col->type = 3;
	}
	join_col-> name = c->args[0];
	join_col->object = (void *) db_cols[ind_col]; 
	join_col->sec_obj = temp_cols[ind_tmp];

	if (join_col1 == NULL)
		join_col1 = join_col;
	else if (join_col2 == NULL)
		join_col2 = join_col;
	else
		perror("Both join columns occupied. Something wrong!");

	return 0;
}



// create col
int create_col(command * c) //done
{
	int type=2;
	char * name = c->args[1];
	if (strncmp(c->args[2],"\"b+tree\"",8)==0)
	{
		type = 0;
		need_sort = true;
	}	
	else if (strncmp(c->args[2],"\"sorted\"",8)==0){
		type = 1;
		need_sort = true;
	}
		
	// if (type == 0 || type ==1)
	// 	key_ind = db_num_cols;

	if (db_num_cols > MAX_COLS)
	{
		perror("Exceeded the limit for the maximum number of columns for this DB");
		exit(-1);
	}

	db_cols[db_num_cols] = malloc(sizeof(col_data));
	// size_t size_name = strlen(name) * sizeof(char);
	db_cols[db_num_cols]->name = malloc(64);
	// memcpy(db_cols[db_num_cols]->name, name, size_name);
	strcpy(db_cols[db_num_cols]->name, name);
	// db_cols[db_num_cols]->name = name;
	db_cols[db_num_cols]->type = type;

	if (type == 0) // add a B+tree
	{
		node * root = make_leaf();
		bigarr * arr = create_arr();
		db_cols[db_num_cols]->object = root;
		db_cols[db_num_cols]->sec_obj = arr;
		
	}
	else
	{
		bigarr * col = create_arr();
		db_cols[db_num_cols]->object = col;
		db_cols[db_num_cols]->sec_obj = NULL;
	}

	db_num_cols++;

	return 0;
}

int load_db(command *c, int s) //done
{
	// send filename back to client
	int CHARSIZE = sizeof(char);
    char * cmd = malloc(100* CHARSIZE);
    strcpy(cmd,"SENDFILE");

    //name without quotes
    char ch1;
    char * name = c->args[1];
	int i=8,j=0;
	// int i=0,j=0;
	do{
		ch1 =name[j];
		if (ch1!='"')
		{
			cmd[i] = ch1;
			i++;
		}
		j++;
	}while(ch1!= NULL);
	cmd[i]='\0';

	// send the message
    send(s, cmd, 100, 0);
    free(cmd);

	//get the file and save it
	char * fname = get_file(1, s);
	//now open the file and load it (fileparser)
	printf("LOAD%s\n", fname );
	
	FILE *fp;
	fp=fopen(fname, "r");
	// int i,j;
	// printf("FNAME %s\n",cmd );
	// fp=fopen(cmd, "r");

	int line_count=0;
	int col_count=0;
	int ch;

	// get the number of line
	while ((ch=fgetc(fp))!= '\n')
	{
		if (ch ==',')
			col_count++;
	}
	col_count++;

	// variable of col names
	char col_names[col_count][64];

	while ((ch=fgetc(fp))!=EOF)
	{
		if (ch == '\n')
			line_count++;	
	}
	int * whole_table = malloc(line_count * col_count * sizeof(int) );
	// int header_count = 3;
	// int header[header_count];
	// header[0] = col_count;
	// header[1] = line_count;
	// header[2] = temp_var_count;
	// write header metadata
	rewind(fp);
	char buffer[64];

	int num_elems = 0;

	i = 0;
	j=0;

	while ((ch=fgetc(fp))!=EOF)
	{	
		// printf("%s\n",ch );
		buffer[i] = ch;
		i++;
		if (ch == ',' || ch =='\n')
		{
			i--;
			buffer[i] = '\0';
			if (num_elems < col_count)
				strcpy(col_names[num_elems],buffer);
			else
				whole_table[num_elems - col_count] = atoi(buffer);
			num_elems++;
			// reset buffer
			for (j = 0; j < i; j++)
				buffer[j] = 0;
			i=0;
		}
		
	}
	fclose(fp);
	unlink(fname);
	if (need_sort)
		qsort(whole_table, line_count, col_count * sizeof(int), compare);
	need_sort = false;

	int ind=0;
	for (i = 0; i < col_count; ++i)
	{
		// printf("%s\n", col_names[i] );
		// first check if the column is in db
		// for (j = 0; j < db_num_cols; j++)
		// {
		// 	if (strcmp(db_cols[j]->name,col_names[i]) == 0)
		// 	{

		// 		flag = true;
		// 		ind = j;
		// 		// break;
		// 	}

		// }
		ind = find_index_db(col_names[i]);
		
		if ( db_cols[ind]->type == 0) //b+tree
		{
			node * root = db_cols[ind]->object;
			bigarr * col = db_cols[ind]->sec_obj;
			if (col->size !=0)
			{
				perror("Array not empty. Can't bulk load.");
				return 1;
			}
			for (int k = 0; k < line_count; ++k)
			{
				root = insert(root, whole_table[ k * col_count + i] , k);
				insert_unsorted(col, whole_table[k * col_count + i]);

			}
		}
		else //data is sorted so, we can just do unsorted inserts
		{
			bigarr * col = db_cols[ind]->object;
			if (col->size !=0)
			{
				perror("Array not empty. Can't bulk load.");
				return 1;
			}

			for (int k = 0; k < line_count; ++k)
				insert_unsorted(col, whole_table[k * col_count + i]);
		}	
	
	}
	// printf("FIRST ELEM %d\n", ((bigarr * )(db_cols[0]->object))->arr[0] );
	// printf("FIRST ELEM %d\n", whole_table[2]);
	// printf("whole_table %d\n", whole_table[0] );
	free(whole_table);
	return 0;
}

int insert_elem(command * c) //done
{
	char * col_name = c-> args[1];
	int ind = find_index_db(col_name);
	int val = atoi(c->args[2]);

	if (ind == -1)
	{
		perror("column not found!");
		return 1;
	}

	if (db_cols[ind]->type == 0) //case: b+tee
	{
		int pos = db_cols[ind]->sec_obj->size;
		insert(db_cols[ind]->object,val,pos);
		insert_unsorted(db_cols[ind]->sec_obj, val); // for efficient retrieval
	}
	else if (db_cols[ind]->type == 1) //sorted
		insert_sorted((bigarr *) db_cols[ind]->object, val);
	else
		insert_unsorted((bigarr *) db_cols[ind]->object, val);

	return 0;
}

// stream tuple to the client
int print_tuples(command *c, int s)
{
	printf("%s\n", temp_col_names[num_temp_cols-1]);

	int i=0,j=0;
	int num_cols = c->num_args-1;
	// get an array of index from the temp col that matches
	bigarr * col_inds = create_arr();
	for (i = 0; i < num_cols; ++i)
	{
		insert_unsorted(col_inds, find_index_temp(c->args[i+1]));
	}
	// fill the arr with ind
	// bigarr * tmp_col = temp_cols[col_inds->arr[0]];
	int line_count = temp_cols[col_inds->arr[0]]->size;
	printf("%d %d \n", line_count, num_cols );

	int ind = 0,n = 0;
	i=0,j=0;
	int max = line_count * num_cols;

	char num_buf[13];
	int str_len = 0;
	while(ind < max)
	{
		str_len = 0;
		strcpy(str,"");
		while(str_len < BUFF_MAX_SIZE && ind < max)
		{
			if (j == 0)
			{
				strcat(str, "(" );
				str_len++;
			}


			if (j != num_cols -1)
			{
				n = sprintf (num_buf, "%d,",temp_cols[col_inds->arr[j]]->arr[i]);
				strcat(str, num_buf);
				++j;
			}
			else
			{
				n = sprintf (num_buf, "%d)\n",temp_cols[col_inds->arr[j]]->arr[i]);
				strcat(str, num_buf);
				++i;
				j=0;
			}
			str_len += n;
			++ind;
		}

        if (send(s, str, str_len, 0) == -1) 
        {
            perror("send");
            exit(1);

		}
	}

	return 0;
}

/////////////////////////// JOIN FUNCTIONS //////////////////////////////////

/*
* Helper function to determinte the right join and do common pre-processign

we have join_col1,2 where join_col1->object = bigarr with values, and 
join_col1->sec_obj = bigarr with positions
*/

void process_join(command * c, int type)
{
	if (join_col1 == NULL || join_col2 == NULL)
		perror("two join cols don't exist!");

	// parse var names
    char * pch = strtok (c->args[0],",");
    char * v1  = pch;
    pch = strtok(NULL,",");
  	char * v2 = pch;

  	// then do the appropriate join
	if (type == 0)
		hash_join(v1,v2);
	else if (type == 1)
		sort_join(v1,v2);
	else if (type == 2)
		loop_join(v1,v2);
	else if (type == 3)
		tree_join(v1,v2);
	else
	{
		perror("Unknown join command");
		exit(-1);
	}
		
	// free both join vars here
	free(join_col1);
	free(join_col2);
	join_col1 = NULL;
	join_col2 = NULL;
}

void loop_join(char * v1, char * v2) 
{
    // get lazy fetched indices from select
    bigarr * vid_small = join_col1->sec_obj;
    bigarr * vid_big = join_col2->sec_obj;

    // get the original column
    col_data * small_dt = join_col1->object;
    col_data * big_dt = join_col2->object;

    bigarr * small = small_dt->object;
    bigarr * big = big_dt->object;

    if (join_col1->type == 4)
    	small = small_dt->sec_obj;
    if (join_col2->type ==4)
    	big = big_dt->sec_obj;

    // create temp array for outputting
    bigarr * temp1 = create_arr();
    bigarr * temp2 = create_arr();

    // because loading 4 arrays
    size_t step = PAGE_SZ / sizeof(int) /4 ; 
    //cache consciuous
    for (size_t i = 0; i < vid_big->size; i += step)  
    {
        for (size_t j = 0; j < vid_small->size; j += step) 
        {
            for (size_t r = i; r < i + step && r < vid_big->size; r++) 
            {
                for (size_t m = j; m < j + step && m < vid_small->size; m++) 
                {	
                    if (small->arr[vid_small->arr[m]] == big->arr[vid_big->arr[r]]) 
                    {
                		insert_unsorted(temp1, vid_small->arr[m]);            
                		insert_unsorted(temp2, vid_big->arr[r]);            
                    }

                }

            }  

        }

    }

	put_in_temp(v1, temp1); //small
	put_in_temp(v2, temp2); //big

}

#define K 256

// do last few digits
int hash_function(int i) {
    return i & 0xFF;
}

int binary_search_bucket_left(bigarr *col, int num)
{

	int imin =0;
	int imax = col->max_size;
	int imid;
	int step;

	int dist = imax - imin;

	while(dist > 0)
	{
		imid = imin;
		step = dist/2;
		imid += step;

		if (col->arr[2 * imid] < num)
		{
			imin = imid +1;
			dist -= step +1;
		}
		else
			dist = step;

	}
	return 2 * imin;
}

void hash_join(char * v1, char * v2) 
{
    // get lazy fetched indices from select
    bigarr * vid_small = join_col1->sec_obj;
    bigarr * vid_big = join_col2->sec_obj;

    // get the original column
    col_data * small_dt = join_col1->object;
    col_data * big_dt = join_col2->object;

    bigarr * small = small_dt->object;
    bigarr * big = big_dt->object;

    if (join_col1->type == 4)
    	small = small_dt->sec_obj;
    if (join_col2->type ==4)
    	big = big_dt->sec_obj;

    // find the bigger one
    if (vid_small->size > vid_big->size)
    {
    	bigarr * tmp = big;
    	big = small;
    	small = tmp;

    	tmp = vid_big;
    	vid_big = vid_small;
    	vid_small = tmp;

		char tmp_name[64];
		strcpy(tmp_name, v1);
		strcpy(v1,v2);
		strcpy(v2, tmp_name);
    }

    // create temp array for outputting
    bigarr * temp1 = create_arr();
    bigarr * temp2 = create_arr();

    // first pass, just count the number of elements in each bucket
    size_t bucket_count[K] = {0};
    for (size_t i = 0; i < vid_small->size; ++i) 
    {
    	int v = small->arr[vid_small->arr[i]];
        int k = hash_function(v);
        bucket_count[k]++;
    }

    // allocate memory for all the data 
    bigarr * buckets[K];
    for (int i = 0; i < K; ++i) 
    {
        buckets[i] = malloc(sizeof(bigarr));
        buckets[i]->arr = malloc(2 * sizeof(int) * bucket_count[i]);
        buckets[i]->size = 0;
        buckets[i]->max_size = bucket_count[i];
    }

    // insert into buckets
    for (size_t i = 0; i < vid_small->size; ++i) 
    {
    	int v = small->arr[vid_small->arr[i]];
        int k = hash_function(v);
        insert_unsorted_fast(buckets[k],v);
        insert_unsorted_fast(buckets[k],vid_small->arr[i]);
    }

    key_ind = 0; // just making sure...
    // sort
    for (int i = 0; i < K; ++i) 
    {
        if (buckets[i]->size){
	    	qsort(buckets[i]->arr, buckets[i]->max_size , 2 * sizeof(int), compare);            
        } 
            
    }

    for (size_t i = 0; i < vid_big->size; ++i) 
    {
        int v = big->arr[vid_big->arr[i]];
        int k = hash_function(v);
        size_t lb = binary_search_bucket_left(buckets[k], v);
     
        // duplicate detection
        while (lb < buckets[k]->size && buckets[k]->arr[lb] == v) 
        {
            // insert into the two variables
    		insert_unsorted(temp1, buckets[k]->arr[lb+1]);            
    		insert_unsorted(temp2, vid_big->arr[i]); 
            lb+=2;           
        }

    }

	put_in_temp(v1, temp1); //small
	put_in_temp(v2, temp2); //big

}

bigarr * sort_join_helper(bigarr * vids, bigarr * col)
{
	bigarr * res = malloc(sizeof(bigarr));
	res->size = 0;
	res->max_size = vids->size;
	res->arr = malloc(2 * res->max_size * sizeof(int));

	for (int i = 0; i < vids->size; ++i)
	{
		// optimized
		insert_unsorted_fast(res, col->arr[vids->arr[i]]);
		insert_unsorted_fast(res, vids->arr[i]);
	}

	// sort
	key_ind = 0;
	qsort(res->arr, res->max_size, 2 * sizeof(int), compare);

	return res;
}
void sort_join(char* v1, char* v2) 
{
   // get lazy fetched indices from select
    bigarr * vid_small = join_col1->sec_obj;
    bigarr * vid_big = join_col2->sec_obj;

    // get the original column
    col_data * small_dt = join_col1->object;
    col_data * big_dt = join_col2->object;

    bigarr * small = small_dt->object;
    bigarr * big = big_dt->object;

    if (join_col1->type == 4)
    	small = small_dt->sec_obj;
    if (join_col2->type ==4)
    	big = big_dt->sec_obj;

    // create temp array for outputting
    bigarr * temp1 = create_arr();
    bigarr * temp2 = create_arr();

    bigarr * small_sorted = sort_join_helper(vid_small, small);
    bigarr * big_sorted = sort_join_helper(vid_big, big);

    int ind =0;
    for (int i = 0, j=0; i < small_sorted->max_size && j < big_sorted->max_size; )
    {
    	if (small_sorted->arr[2 * i] == big_sorted->arr[2 * j])
    	{
    		ind = j;
    		do
    		{
    			insert_unsorted(temp1, small_sorted->arr[2 * i +1]);
    			insert_unsorted(temp2, big_sorted->arr[2 * j +1]);
	    		j++;
    		}
    		while(small_sorted->arr[2 * i] == big_sorted->arr[2 * j]);
    		
    		// go back to the beginnig of the block
    		j = ind;
    		i++;
    	}
    	else if (small_sorted->arr[2 * i] > big_sorted->arr[2 * j])
    		j++;
    	else
    		i++;
    }

	put_in_temp(v1, temp1); //small
	put_in_temp(v2, temp2); //big
}

void tree_join(char * v1, char * v2) 
{

	// if none of the two cols are trees it is strictly better to use hashjoin
	if (join_col1->type != 4 && join_col2->type !=4)
	{
		printf("HASH JOIN\n");
		hash_join(v1, v2);		
	}
	else
	{

		// small = B+tree
		if (join_col1->type != 4)
		{
			col_data * tmp = join_col1;
			join_col1 = join_col2;
			join_col2 = tmp;

			char tmp_name[64];
			strcpy(tmp_name, v1);
			strcpy(v1,v2);
			strcpy(v2, tmp_name);

		}

	    // get lazy fetched indices from select
	    bigarr * vid_small = join_col1->sec_obj;
	    bigarr * vid_big = join_col2->sec_obj;

	    // get the original column
	    col_data * small_dt = join_col1->object;
	    col_data * big_dt = join_col2->object;

	    node * small = small_dt->object;
	    bigarr * big = big_dt->object;

	    // the array for reverse mapping
	    bigarr * tree_arr = small_dt->sec_obj;

	    // create temp array for outputting
	    bigarr * temp1 = create_arr();
	    bigarr * temp2 = create_arr();

	    bigarr * bitvec = malloc(sizeof(bigarr));
	    bitvec->arr=malloc(tree_arr->size * sizeof(int));
	    bitvec->size = tree_arr->size;
	    bitvec->max_size=tree_arr->size;

	    // make bitvector from selection
	    for (int i = 0; i < bitvec->max_size; ++i)
	    	bitvec->arr[i] = 0;
	    for (int i = 0; i < vid_small->size; ++i)
	    	bitvec->arr[vid_small->arr[i]] = 1;
	    
	    record * r;
		for (int i = 0; i < vid_big->size; ++i)
		{
			int v_big = big->arr[vid_big->arr[i]];
			// printf("%d\n", v_big);
			if ((r = find(small, v_big) )!= NULL)
			{
				// printf("%d\n", r->value);
				//check the bitvector
				if (bitvec->arr[r->value] ==1)
				{
					// insert to temp
					do
					{
						insert_unsorted(temp1, r->value);            
	    				insert_unsorted(temp2, vid_big->arr[i]); 
					}while((r=r->next) != NULL);
				}
			}

		}

		put_in_temp(v1, temp1); //small
		put_in_temp(v2, temp2); //big
	}
}

/////////////////// UPDATES: P4 //////////////////////

void delete_col(bigarr * posArr, char * col_name)
{
	int ind = find_index_db(col_name);
	bigarr * col = db_cols[ind]->object;

	bigarr * new_col = malloc(sizeof(bigarr));
	new_col->size = 0;
	new_col->max_size = col->size - posArr->size;
	new_col->arr = malloc(new_col->max_size * sizeof(int));

	int del_ind = 0;
	int next_del = posArr->arr[del_ind];

	for (int i = 0; i < col->size; ++i)
	{
		if (i == next_del)
			next_del = posArr->arr[++del_ind];
		else
			insert_unsorted(new_col, col->arr[i]);

	}

	db_cols[ind]->object = new_col;
	free_arr(col);
}

void delete_master(command * c) 
{
    int tmp_ind = find_index_temp(c->args[1]);
    bigarr * posArr = temp_cols[tmp_ind];

    for (int i = 2; i < c->num_args; ++i)
    	delete_col(posArr, c->args[i]);

}

void update_col(command * c) 
{
    
    int tmp_ind = find_index_temp(c->args[1]);
    bigarr * posArr = temp_cols[tmp_ind];

    int ind = find_index_db(c->args[2]);
    bigarr * col = db_cols[ind]->object;

    int val = atoi(c->args[3]);

    for (int i = 0; i < posArr->size; ++i)
    	col->arr[posArr->arr[i]] = val;
}

void insert_column(char *col_name, int v) 
{
	int ind = find_index_db(col_name);
	insert_unsorted(db_cols[ind]->object, v);
}

void insert_master(command *c)
{
	for (int i = 0; i < c->num_args/2; ++i)
		insert_column(c->args[2 * i +1], atoi(c->args[2 *i +2]));
}


// master execution function
void execute_cmd(command * c, int s)
{

	switch(c->type)
	{
		case 0:
			select_col(c);
			break;
		case 1:
			select_elem(c);
			break;
		case 2:
			select_range(c);
			break;
		case 3:
			fetch1(c,s);
			break;
		case 4:
			create_col(c);
			break;
		case 5:
			load_db(c,s);
			break;
		case 6:
			insert_elem(c);
			break;
		case 7:
			print_tuples(c,s);
			break;
		case 8:
			fetch2(c,s);
			break;
		case 9:
			lazy_fetch(c,s);
			break;
		case 10:
			process_join(c,3);
			break;
		case 11:
			process_join(c,0);
			break;
		case 12:
			process_join(c,2);
			break;
		case 13:
			process_join(c,1);
			break;
		case 14:
			min_col(c);
			break;
		case 15:
			max_col(c);
			break;
		case 16:
			sum_col(c);
			break;
		case 17:
			avg_col(c);
			break;
		case 18:
			count_col(c);
			break;
		case 19:
			add_col(c);
			break;
		case 20:
			sub_col(c);
			break;
		case 21:
			mul_col(c);
			break;
		case 22:
			div_col(c);
			break;
		case 23:
			insert_master(c);
			break;
		case 24:
			delete_master(c);
			break;
		case 25:
			update_col(c);
			break;
		case 26:
			cache_set(c);
			break;
		case 27:
			close_server();
			break;
		default:
			printf("SOMETHING IS WRONG!\n");
			break;
	}

}