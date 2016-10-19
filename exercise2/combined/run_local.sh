#!/bin/bash

max=$1
if [[ $max == "" ]]
then
  max=40
fi

killall lua
sleep 1

for (( n=1;n<=$max;n++ ))
do
  lua gossip.lua $n $max &
done
