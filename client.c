#define _GNU_SOURCE
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h> 
#include <unistd.h>

#define SOCK_PATH "echo_socket"
#define BUFF_MAX_SIZE 524288 //512KB Buffer
int DEBUG = false;
char str[BUFF_MAX_SIZE];

int send_file(int flag, int s, char * filename)
{
    int n=0;
    FILE * fp;

    if (flag ==0) //sendcmd
    {

         fp = stdin;
    }
    else //senddata
    {

        fp = fopen(filename, "r");   
    }

    while (!feof(fp))
    {
        n = fread(str, sizeof(char), BUFF_MAX_SIZE, fp);
        // printf("%s\n",str );
        if (n<BUFF_MAX_SIZE)
            str[n] ='\0';
        if (send(s, str, n, 0) == -1) {
            perror("send");
            exit(1);
        }
    }
    if (send(s, "|", 1, 0) == -1) {
            perror("send");
            exit(1);
        }
    if (DEBUG)
        printf("DELIMETER SENT\n");
    if (flag ==1)
        fclose(fp);

    return 0;
}

int main(int argc, char const *argv[])
{
    int s, n, len;
    struct sockaddr_un remote;


    if ((s = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        perror("socket");
        exit(1);
    }

    if (DEBUG)
        printf("Trying to connect...\n");

    remote.sun_family = AF_UNIX;
    strcpy(remote.sun_path, SOCK_PATH);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family);

    if (connect(s, (struct sockaddr *)&remote, len) == -1) {
        perror("connect");
        exit(1);
    }

    if (DEBUG)
        printf("Connected to the server.\n");

    send_file(0,s,"none");

    
    if (DEBUG)
        printf("CMD FILE SENT\n");
    //reset 

    while(true)
    {
        n = recv(s, str, BUFF_MAX_SIZE, 0);
        if (n <=0)
        {
            if (n < 0)
            {
                perror("recv");
                if (DEBUG)
                    printf("Stopped receiving.\n");
                break;
            } 
        }
        else if (strncmp(str,"SENDFILE",8) ==0 )
        {
            str[n]='\0';
            char * fname = str+8;
            if (DEBUG)
                printf("HERE FNAME %s\n",fname );
            send_file(1,s,fname);
            // printf("REACHED BOTTOM\n");
        }
        else if (strncmp(str,"EXIT",4) ==0 )
        {
            break;
        }
        else
        {
            if (n < BUFF_MAX_SIZE)
                str[n] = '\0';
            printf("%s",str);

            char * ext = malloc(4);
            memcpy(ext,str + (n+1), 4);
            // printf("%s\n", ext);
            if (strncmp(ext,"EXIT",4) ==0 )
                break;
        }
    }
    close(s);

    return 0;
}
