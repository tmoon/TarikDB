#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
int key_ind = 0;

int compare (const void * a, const void * b)
{
//keep key_ind global and change before use
  return ( *((int*)a + key_ind) - *((int*)b + key_ind) );
}

int main(void)
{	
// file write stuff
	/*
	FILE * fp = fopen("\"test.txt\"","wb");
	int * arr = malloc(100*sizeof(int));
	*arr = 10001;
	fwrite(arr, sizeof(int),1,fp);

	char * name = "\"b+tree\"";
	if (!strncmp(name, "\"b+tree\"",8))
		printf("WORKS!\n");
	char * cmd = malloc(64);
	char ch;
	int i=0,j=0, count=0;
	do{
		ch =name[j];
		printf("%c\n",ch );
		if (ch!='"')
		{
			cmd[i] = ch;
			i++;
		}
		j++;
	}while(ch!= NULL);
	cmd[i]="\0";
	printf("%s\n",cmd );


	fclose(fp);
	fp = fopen("\"test.txt\"","rb");
	int buff[10];
	fread(buff, sizeof(int), 1, fp);
	printf("%d\n",buff[0]);
*/
	

	char ** arr = malloc(10* sizeof(char *));
	arr[0] = "This";
	arr[1] = "is";
	arr[2] = "nice";
	printf("%s\n",arr[2] );
	int table[10][3];
	for (int row=0; row<10; row++)
	{
		for (int col=0; col<3; col++)
		{

			table[row][col] = abs(2*row - 3*col);
			printf("%d ", table[row][col]);
		}
		printf("\n");
	}

printf("SORTED \n");
qsort(table, 10, 3 * sizeof(int), compare);

	for (int row=0; row<10; row++)
	{
		for (int col=0; col<3; col++)
		{
			printf("%d ", table[row][col]);
		}
		printf("\n");
	}
	return 0;
}
