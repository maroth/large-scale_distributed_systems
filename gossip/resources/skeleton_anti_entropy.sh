#!/bin/bash

max=$1
if [[ $max == "" ]]
then
  max=10
fi

killall lua
sleep 1

for (( n=1;n<=$max;n++ ))
do
  rm $n.log > /dev/null 2>&1
  # lua anti_entropy.lua $n $max > $n.log 2>&1 &
  lua skeleton_anti_entropy.lua $n $max &
done
