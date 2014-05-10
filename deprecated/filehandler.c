#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// int binary_search(char * col, char * name)
// {
// 	int imin =0;
// 	int imax = col->size-1;
// 	int imid=0;

// 	if (col->arr[imax] < num)
// 		return -1;

// 	while(imax >= imin)
// 	{
// 		imid = (imin + imax)/2;
// 		if (col->arr[imid] == num)
// 			return imid;
// 		else if (col->arr[imid] < num)
// 			imin = imid + 1;
// 		else if (col->arr[imid] > num)
// 			imax = imid - 1;
// 	}
// 	return imid;
// }



int main(int argc, char const *argv[])
{
	FILE *fp;
	fp=fopen("p2table1.csv", "r");

	int line_count=0;
	int num_cols=1;
	int ch;

	// get the number of line
	while ((ch=fgetc(fp))!= '\n')
	{
		if (ch ==',')
			num_cols++;
	}

	char col_names[num_cols][64];

	while ((ch=fgetc(fp))!=EOF)
	{
		if (ch == '\n')
			line_count++;	
	}
	
	// we are using 2d array because it seems that we need to materialize
	int table_cols[line_count][num_cols];

	rewind(fp);

	char buffer[64];

	int col = 0;
	int row = 0;
	int i = 0;
	int j=0;
	while ((ch=fgetc(fp))!=EOF)
	{	
		// printf("%s\n",ch );
		buffer[i] = ch;
		i++;
		if (ch == ',' || ch =='\n')
		{
			i--;
			buffer[i] = '\0';
			if (row == 0)
				memcpy(col_names[col],buffer, 64* sizeof(char));
			else
				table_cols[row-1][col] = atoi(buffer);

			if (ch =='\n')
			{
				col = 0;
				row++;
			}
			else
				col++;

			for (j = 0; j < i; j++)
				buffer[j] = 0;
			i=0;
			// printf("%s\n", buffer );
		}
	}
	


	fclose(fp);
	for (i=0; i < line_count; i++)
	{
		for (int j=0; j<num_cols; j++)
			printf("%d ",table_cols[i][j]);
		printf("\n");
	}
	printf("%s\n", col_names[2] );
	printf("%d %d\n", line_count, num_cols);
	return 0;
}


