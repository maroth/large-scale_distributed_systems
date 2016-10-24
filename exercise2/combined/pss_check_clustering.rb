#!/usr/bin/env ruby

# this script parses the output of a concatenation of your log files, or
# better, a grepped-out concatenation with only the lines that are of interest
# 
# the lines should look like:
# (something, we don't care) VIEW_CONTENT 4 9 13 8 47 18 10 27 34 43 30
# where 4 is the index of the node issuing its view content
# and the following are the id of the nodes in its view

if ARGV.size != 1
  puts "Usage: ruby pss_check_clustering.rb FILENAME"
  exit()
end

file = ARGV[0]

if !File.exists?(file)
  puts "file #{file} not found"
  exit()
end

def read_views(file)
  views = Hash.new
  File.open(file).each do |line|
    if (line =~ /.*VIEW_CONTENT/)
      _t = line.split("VIEW_CONTENT")[1].split()
      peer = _t[0].to_i
      if views[peer] then
        puts("Warning: peer #{peer} printed its view several times -- only counting the first occurence.")
      else
        views[peer] = Array.new
        1.upto(_t.size-1) do |i|
          views[peer] << _t[i].to_i
        end
      end      
    end
  end
  views
end

####
# output the distribution of clustering

clustering=Array.new
views = read_views(file)
views.each do |p,neighbors|
  nbpairs = 0.0
  nbclusteredpairs = 0.0
  neighbors.each do |n|
    neighbors.each do |m|
      if n != m then
        nbpairs = nbpairs + 1.0
        # is this neighbor a neighbor of this other neighbor? :)
        if views[n].include?(m) then
          nbclusteredpairs = nbclusteredpairs + 1.0
        end          
      end
    end
  end
  clustering << (nbclusteredpairs / nbpairs)
end

puts("# clustering cumul_peers")
cumul = 0
clustering.sort.each do |c|
  cumul = cumul + 1
  puts("#{c} #{cumul}")
end
