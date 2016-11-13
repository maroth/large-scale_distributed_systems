set term epslatex # font "FreeMono, 10"

set style line 1 lt 1 lc rgb "#FF0000" lw 7 # red
set style line 2 lt 1 lc rgb "#00FF00" lw 7 # green
set style line 3 lt 1 lc rgb "#0000FF" lw 7 # blue
set style line 4 lt 1 lc rgb "#000000" lw 7 # black
set style line 5 lt 1 lc rgb "#CD00CD" lw 7 # purple
set style line 7 lt 3 lc rgb "#000000" lw 7 # black, dashed line

set output "search_performance.eps"
set title "Search Performance without Fingers"

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
set yrange [0:] # example of a closed range (points outside will not be displayed)
set xrange [-1:64] # example of a range closed on one side only, the max will determined automatically

plot "search_performance.log" u ($1):($2) with boxes title ""
