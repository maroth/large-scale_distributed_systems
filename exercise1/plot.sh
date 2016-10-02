#!/bin/bash
FILE=$1
echo "Input file: ${FILE}"
gnuplot -persist << EOF
set term post eps
set output "${FILE}.eps"
set title "Data from ${FILE}"
set xdata time
set timefmt "%H:%M:%S"
set format x "%H:%M:%S"
set xlabel "time"
set ylabel "% load"
# it will always fit in this range
set xrange ["00:00:00":"23:59:59"]
set yrange [1:110]
set ytics 20
plot "${FILE}" u 1:2 with lines title "Load"
pause -1
EOF
