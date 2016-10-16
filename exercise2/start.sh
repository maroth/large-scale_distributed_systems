#!/bin/bash
 
max=50
 
killall lua
sleep 1
 
for (( n=1;n<=$[$max];n++ )); do
  rm $n.log > /dev/null 2>&1
  lua intro.lua $n $max > $n.log 2>&1 &
done
