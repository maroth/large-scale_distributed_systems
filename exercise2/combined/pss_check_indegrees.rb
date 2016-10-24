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
# output the indegree distribution
views=read_views(file)
indegrees=Hash.new
views.each do |key,value|
  if ! indegrees[key] then indegrees[key] = 0 end
  value.each do |v|
    if ! indegrees[v] then indegrees[v] = 0 end
    indegrees[v] = indegrees[v] + 1 
  end  
end
# calculate distribution
distindegrees=Hash.new
indegrees.each do |k,v|
  if !distindegrees[v] then distindegrees[v] = 0 end
  distindegrees[v] = distindegrees[v] + 1
end
# output indegrees sorted
cdf=0
puts("# indegree peers cumul")
distindegrees.sort{|a,b| a[0]<=>b[0]}.each do |k,v| 
  cdf = cdf + v
  puts("#{k} #{v} #{cdf}")
end
