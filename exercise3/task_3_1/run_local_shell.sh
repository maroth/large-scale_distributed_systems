#!/bin/bash

max=$1
if [[ $max == "" ]]
then
  max=64
fi

killall lua
sleep 1

rm log.txt
touch log.txt
for (( n=1;n<=$max;n++ ))
do
  lua chord.lua $n $max & 
done
