#!/bin/bash
rm logserver
rm logclient
echo "" > autolog
echo "" > lock_server.log
for ((i=0;i<500;i++));
  do 
    echo $i >> autolog
    export RPC_LOSSY=0
    echo $RPC_LOSSY >> autolog
    killall lock_server
    killall lock_tester
    ./lock_server 50000 >> lock_server.log &
    time ./lock_tester 50000
    export RPC_LOSSY=5
    echo $RPC_LOSSY >> autolog
    killall lock_server
    killall lock_tester
    ./lock_server 50000 >> lock_server.log &
    time ./lock_tester 50000
    if (($? != 0))
      then
        break;
    fi
  done
