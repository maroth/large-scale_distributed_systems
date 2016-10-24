set term epslatex # font "FreeMono, 10"

# some line types with different colors, you can use them by using line styles in the plot command afterwards (linestyle X)
set style line 1 lt 1 lc rgb "#FF0000" lw 7 # red
set style line 2 lt 1 lc rgb "#00FF00" lw 7 # green
set style line 3 lt 1 lc rgb "#0000FF" lw 7 # blue
set style line 4 lt 1 lc rgb "#000000" lw 7 # black
set style line 5 lt 1 lc rgb "#CD00CD" lw 7 # purple
set style line 7 lt 3 lc rgb "#000000" lw 7 # black, dashed line

set output "plots/plot_4-2.eps"
set title "Dissemination Comparison"

# indicates the labels
set xlabel "Cycles"
set ylabel "Peers Infected"

set size 1.0, 1.0

# set the grid on
set grid x,y

# set the key, options are top/bottom and left/right
set key bottom right

# indicates the ranges
set yrange [0:50] # example of a closed range (points outside will not be displayed)
set xrange [2:12] # example of a range closed on one side only, the max will determined automatically

plot "aggregated_log_4-2_h0s0.txt" u ($2):($3) with lines linestyle 1 title "H=0 S=0 (19 duplicates)", \
     "aggregated_log_4-2_h1s1.txt" u ($2):($3) with lines linestyle 2 title "H=1 S=1 (19 duplicates)", \
     "aggregated_log_4-2_h1s4.txt" u ($2):($3) with lines linestyle 3 title "H=1 S=4 (22 duplicates)", \
     "aggregated_log_4-2_h4s0.txt" u ($2):($3) with lines linestyle 4 title "H=4 S=0 (19 duplicates)", \
     "aggregated_log_4-2_h4s1.txt" u ($2):($3) with lines linestyle 5 title "H=4 S=1 (19 duplicates)", \
     "aggregated_log_4-2_h0s4.txt" u ($2):($3) with lines linestyle 6 title "H=0 S=4 (22 duplicates)", \
     "aggregated_log_4-2_no_pss.txt" u ($2):($3) with lines linestyle 7 title "No Peer Sampling (87 duplicates)"

