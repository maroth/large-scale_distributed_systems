#!/usr/bin/python
import os
from collections import defaultdict
aggregated = defaultdict(int)
with open("log.txt", "r") as log:
    lines = log.readlines()
    for line in lines:
        if "key_found" in line:
            aggregated[(line.split(" ")[-1].rstrip())] += 1

sum = 0
with open("search_performance.log", "w") as outputfile:
    for hops, count in sorted(aggregated.iteritems(), key=lambda(k, v): int(k)):
        sum += count        
        outputfile.write("{} {}\n".format(hops, count))
        print("{} {}".format(hops, count))

    print("total: {}".format(sum))

os.system("gnuplot search_performance.gp")
