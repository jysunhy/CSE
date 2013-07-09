#!/bin/bash

rm logserver
rm logclient
echo "" > autolog
echo "" > lock_server.log
for ((i=0;i<100;i++));
  do 
#  ./stop.sh
#  ./stop.sh
    echo $i >> autolog
    export RPC_LOSSY=0
    echo $RPC_LOSSY >> autolog
  ./stop.sh
  ./start.sh
#  ./test-lab-2-a.pl yfs1
#  ./test-lab-2-b.pl yfs1 yfs2
#  ./test-lab-3-a.pl yfs1
   time ./test-lab-3-b yfs1 yfs2
#  ./test-lab-3-c yfs1 yfs2
  ./stop.sh
    export RPC_LOSSY=5
    echo $RPC_LOSSY >> autolog
  ./stop.sh
  ./start.sh
#  ./test-lab-2-a.pl yfs1
#  ./test-lab-2-b.pl yfs1 yfs2
#  ./test-lab-3-a.pl yfs1
   time ./test-lab-3-b yfs1 yfs2
#  ./test-lab-3-c yfs1 yfs2
  ./stop.sh
  done
