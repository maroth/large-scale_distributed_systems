set term epslatex # font "FreeMono, 10"

# some line types with different colors, you can use them by using line styles in the plot command afterwards (linestyle X)
set style line 1 lt 1 lc rgb "#FF0000" lw 7 # red
set style line 2 lt 1 lc rgb "#00FF00" lw 7 # green
set style line 3 lt 1 lc rgb "#0000FF" lw 7 # blue
set style line 4 lt 1 lc rgb "#000000" lw 7 # black
set style line 5 lt 1 lc rgb "#CD00CD" lw 7 # purple
set style line 7 lt 3 lc rgb "#000000" lw 7 # black, dashed line

set output "plots/plot_2-3.eps"
set title "Rumor Mongering and Anti-entropy comparison on 40 Peers."

# indicates the labels
set xlabel "Cycles"
set ylabel "Peers Infected"

set size 1.0, 1.0

# set the grid on
set grid x,y

# set the key, options are top/bottom and left/right
set key bottom right

# indicates the ranges
set yrange [0:40] # example of a closed range (points outside will not be displayed)
set xrange [0:] # example of a range closed on one side only, the max will determined automatically

plot "aggregated_log_2-3.txt" u ($2):($3) with lines linestyle 1 title "Total infections", \
     "aggregated_log_2-3.txt" u ($2):($6) with lines linestyle 2 title "Infected by Anti-entropy", \
     "aggregated_log_2-3.txt" u ($2):($7) with lines linestyle 3 title "Infected by Rumor Mongering"


# $1 is column 1. You can do arithmetics on the values of the columns
