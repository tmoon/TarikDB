#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdbool.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "all_structs.h"
#include "worker.h"

#define SOCK_PATH "echo_socket"
#define NUM_CONS 10
#define BUFF_MAX_SIZE 524288 //512KB Buffer

#define DEBUG true

char str[BUFF_MAX_SIZE];


int main(int argc, char *argv[]) {

  int s, len;
  socklen_t  t;
  struct sockaddr_un local,remote;

  if ((s = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
      perror("socket");
      exit(1);
  }

  local.sun_family = AF_UNIX;
  strcpy(local.sun_path, SOCK_PATH);
  unlink(local.sun_path);
  len = strlen(local.sun_path) + sizeof(local.sun_family);

  if (bind(s, (struct sockaddr *)&local, len) == -1) {
      perror("bind");
      exit(1);
  }

  if (listen(s, NUM_CONS) == -1) {
      perror("listen");
      exit(1);
  }
  

for(;;) {

  int s2;
  pthread_t thread;
  if (DEBUG)
      printf("Waiting for a connection...\n");
  t = (socklen_t ) sizeof(remote);

  if ((s2 = accept(s, (struct sockaddr *)&remote, &t)) == -1) {
      perror("accept");
      exit(1);
  }
  else 
  {
      if (DEBUG)
          printf("Connected to %d.\n", s2);
        
      if (pthread_create(&thread, NULL, thread_handler, &s2) != 0) 
          fprintf(stderr, "Failed to create thread\n");
      
  }
}
 
 
   pthread_exit(NULL);
}