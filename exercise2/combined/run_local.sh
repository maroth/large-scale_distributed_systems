#!/bin/bash

max=$1
if [[ $max == "" ]]
then
  max=40
fi

killall lua
sleep 1

rm log.txt
touch log.txt
for (( n=1;n<=$max;n++ ))
do
  lua gossip.lua $n $max >> log.txt &
done
