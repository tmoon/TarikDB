 /* 
 *  A threaded server
 *  by Martin Broadhurst (www.martinbroadhurst.com)
 *  Compile with -pthread
 */

#include <stdio.h>
#include <string.h> /* memset() */
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <netdb.h>
#include <pthread.h>

#define SOCK_PATH "echo_socket"
#define PORT    "32001" /* Port to listen on */
#define BACKLOG     10  /* Passed to listen() */

void *handle(void *pnewsock)
{
    /* send(), recv(), close() */
    char str[100];

    int *myarg;

    myarg = (int *)pnewsock;
    int server = *myarg;

    int n = recv(server, str, 100, 0);
            if (n <= 0) {
                if (n < 0) perror("recv");
            }
            str[n] ='\0';
  printf("got this: %s", str);
 
  close(server);

    return NULL;
}

int main(void)
{
    int s, s2, t, len;
    struct sockaddr_un local, remote;
    char str[100];

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

    /* Main loop */
    while (1) {
        size_t size = sizeof(struct sockaddr_in);
        struct sockaddr_in their_addr;
        int newsock = accept(sock, (struct sockaddr*)&their_addr, &size);
        if (newsock == -1) {
            perror("accept");
        }
        else {
            printf("Got a connection from %s on port %d\n",
                    inet_ntoa(their_addr.sin_addr), htons(their_addr.sin_port));
            if (pthread_create(&thread, NULL, handle, &newsock) != 0) {
                fprintf(stderr, "Failed to create thread\n");
            }
        }
    }

    close(sock);

    return 0;
}

