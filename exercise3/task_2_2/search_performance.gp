set term png # font "FreeMono, 10"

set style line 1 lt 1 lc rgb "#FF0000" lw 7 # red
set style line 2 lt 1 lc rgb "#00FF00" lw 7 # green
set style line 3 lt 1 lc rgb "#0000FF" lw 7 # blue
set style line 4 lt 1 lc rgb "#000000" lw 7 # black
set style line 5 lt 1 lc rgb "#CD00CD" lw 7 # purple
set style line 7 lt 3 lc rgb "#000000" lw 7 # black, dashed line

set output "search_performance.png"
set title "Search Performance with Fingers"

# indicates the labels
set xlabel "Hops"
set ylabel "# Searches"

# set the grid on
set grid x,y

# set the key, options are top/bottom and left/right
set key top left

# indicates the ranges
set boxwidth 0.6
set style fill solid 1.0
set yrange [0:] 
set xrange [-1:7] 

plot "search_performance.log" u ($1):($2) with boxes title ""
