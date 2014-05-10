 echo "COMPILING CLIENT"
 gcc -std=c99 -Wall -I ./includes/ client.c -o client
 echo "CLIENT COMPILED"
 echo "COMPILING THREADED SERVER"
 gcc -std=c99 -Wall -I ./includes/ -pthread bplustree.c bigarr.c worker.c server.c -o server
 echo "SERVER COMPILED"