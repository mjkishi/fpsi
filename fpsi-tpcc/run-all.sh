#!/bin/bash

#for server in $(cat cluster.txt); do
#       scp name-jar.jar palmieri@$server:~/score2blablabla
#done
#echo "Copy done!"


### Kill all Java process
#for server in $(cat cluster.txt); do
#       ssh -o "StrictHostKeyChecking no" palmieri@$server "killall java" 
#done
#echo "Done kill java"

if [[ $1 = "" ]]; then
        echo "Missing commands!"
        exit
fi

for server in $(cat cluster.txt); do
        ssh -o "StrictHostKeyChecking no" palmieri@$server "$1" | tee results/$server.txt
        echo "Run $1 on $server"
done
echo "Run on all nodes"
