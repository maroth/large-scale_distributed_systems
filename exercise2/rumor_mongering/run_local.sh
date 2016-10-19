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
  lua anti_entropy.lua $n $max &
done
