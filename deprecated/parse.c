#define _GNU_SOURCE
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "all_structs.h"
#include "parse.h"
#define BUFF_MAX_SIZE 524288 //512KB Buffer

#define DEBUG true

char str[BUFF_MAX_SIZE];


command * parse_cmd(char * line)
{
  command * cmd = malloc(sizeof(command));
  int num_input = 0, input_count=4; //allocating slightly more memory for later use
  int type = 0,i=0;
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
    //then it is a select or fetch operation
    pch = strtok (line,"=");

    while(pch!=NULL)
    {
      cmd->args[num_input] = pch;
      if (num_input == 1)
      {
        if (strncmp(pch,"select", 6) == 0)
          type = 0;
        else if (strncmp(pch,"fetch", 5) == 0)
          type = 8;
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
          type = 6;
        else if (strncmp(pch,"tuple",5)==0)
          type = 7;
        else
          printf("Unknown function entered: %s\n", pch);
      }

      num_input++;
      pch = strtok  (NULL, "(,)\n");
    }
  }


  cmd->num_args = num_input;

  cmd->type = type;
  return cmd;
}


// int main(int argc, char const *argv[])
// {
// // from: http://stackoverflow.com/questions/3495092/read-from-file-or-stdin-c

//    FILE * fp;
//    char * line = NULL;
//    size_t len = 0;
//    ssize_t read;

//    fp = stdin;
//    if (fp == NULL)
//        exit(EXIT_FAILURE);

//    // char *temp_name[10];
//    // int temp_ptr[10];

//    while ((read = getline(&line, &len, fp)) != -1) {
//        // printf("Retrieved line of length %zu :\n", read);
//        printf("%s", line);
//       command * c = parse_cmd(line);
//       printf("%d\n",c->type );
//       for (int i = 0; i < c->num_args; ++i)
//       {
//         printf("%s\n", (char *) c->args[i] );
//       }



     

// 	  // char * pch;
// 	  // pch = strtok (line,"=");
// 	  // while (pch != NULL)
// 	  // {
// 	  //   printf ("%s\n",pch);
// 	  //   pch = strtok (NULL, "=");
// 	  // }
//    }

//    if (line)
//        free(line);   



//   // char str[] ="- This, a sample string.";

//   return 0;
// }

/*
Things to consider:
1. for each line, check if it has = sign (i.e. we need variable name)
2. if not then just split by "(" and get the command
3. will have at most three input: so get rid of ")" and split by ','
*/