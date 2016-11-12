set term epslatex # font "FreeMono, 10"

# some line types with different colors, you can use them by using line styles in the plot command afterwards (linestyle X)
set style line 1 lt 1 lc rgb "#FF0000" lw 7 # red
set style line 2 lt 1 lc rgb "#00FF00" lw 7 # green
set style line 3 lt 1 lc rgb "#0000FF" lw 7 # blue
set style line 4 lt 1 lc rgb "#000000" lw 7 # black
set style line 5 lt 1 lc rgb "#CD00CD" lw 7 # purple
set style line 7 lt 3 lc rgb "#000000" lw 7 # black, dashed line

set output "plots/plot_3-2_h0s4_indegrees.eps"
set title "In-degree Distribution for H=0 S=4"

# indicates the labels
set xlabel "In-Degree"
set ylabel "Number of Nodes"

set size 1.0, 1.0

# set the grid on
set grid x,y

# set the key, options are top/bottom and left/right
set key top right

set style line 1 lc rgb "red"
set style line 2 lc rgb "blue"

set style fill solid
set boxwidth 0.5

# indicates the ranges
set yrange [0:] # example of a closed range (points outside will not be displayed)
set xrange [0:] # example of a range closed on one side only, the max will determined automatically

plot "log_3-2_check_indegrees_output_h0s4.txt" u ($1):($2) with boxes title ""