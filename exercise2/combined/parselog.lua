------------------------------------
-- Parsing and Aggregating
------------------------------------

-- split the lines in the log file and assign the cleaned up values to tuples
function parse(lines)
  local tuples = {}

  --names for the different numbers we have in the log file
  local fields = {"Date", "Timestamp", "Node", "Message"}

  for i, line in ipairs(lines) do
    local tuple = {}
    local count = 1
      for element in string.gmatch(line, "%S+") do
        
        -- for the Node field, we need to strip the parens
        if fields[count] == "Node" then
          element = tonumber(string.sub(element, 2, -2))
        end

        tuple[fields[count]] = element
        count = count + 1
      end

    -- to parse the datetime field, we need to combine the Date and Timestamp fields
    tuple["ParsedTime"] = parse_timestamp(tuple["Date"] .. " " .. tuple["Timestamp"])

    tuples[i] = tuple
  end
  return tuples
end

-- aggregate the elements read from the log file
function aggregate(tuples)
  local elements = {}
  local starting_time = tuples[1]["ParsedTime"]
  local absolute_infected_nodes = 0
  local nodes_infected_by_anti_entropy = 0
  local nodes_infected_by_rumor_mongering = 0
  local duplicates = 0
  for i, tuple in ipairs(tuples) do
    relative_time = tuple["ParsedTime"] - starting_time

    if string.starts(tuple["Message"], "i_am_infected") then
      absolute_infected_nodes = absolute_infected_nodes + 1
      relative_infected_nodes = absolute_infected_nodes / number_of_nodes
    end

    if tuple["Message"] == "i_am_infected_by_rumor_mongering" then
      nodes_infected_by_rumor_mongering = nodes_infected_by_rumor_mongering + 1
    end

    if tuple["Message"] == "i_am_infected_by_anti_entropy" then
      nodes_infected_by_anti_entropy = nodes_infected_by_anti_entropy + 1
    end

    if tuple["Message"] == "duplicate_received" then
      duplicates = duplicates + 1
    end

    element = {relative_time, 
                absolute_infected_nodes, 
                relative_infected_nodes, 
                duplicates, 
                nodes_infected_by_anti_entropy, 
                nodes_infected_by_rumor_mongering}
    table.insert(elements, element)
  end
  return elements
end

-- save the agregated elements into a file
function save_file(elements, filename)
  file = io.open(filename, "w")
  for i, element in ipairs(elements) do
    local line = ""
    for entry in pairs(element) do
      line = line .. " " .. element[entry]
    end
    line = line .. "\n"
    file:write(line)
  end
  file:close()
end


------------------------------------
-- Helper Methods
------------------------------------

--parse timestamp strings with the format "2016-10-16 14:33:41"
function parse_timestamp(timestamp)
  local pattern = "(%d+)%-(%d+)%-(%d+) (%d+):(%d+):(%d+).(%d+)"
  local xyear, xmonth, xday, xhour, xminute, xseconds, xmillis = timestamp:match(pattern)
  local convertedTimestamp = os.time({year = xyear, month = xmonth, day = xday, hour = xhour, min = xminute, sec = xseconds})
  return convertedTimestamp
end


-- does a file exist?
function file_exists(file) 
  local f = io.open(file, "rb")
  if f then
    f:close() 
  end
  return f ~= nil
end


-- read lines from a a file
function read_lines_from_file(file)
  if not file_exists(file) then 
    return {} 
  end
  lines = {}
  for line in io.lines(file) do 
    lines[#lines + 1] = line
  end
  return lines
end

-- checks whether a string starts with another string
-- copied from the internet: http://lua-users.org/wiki/StringRecipes
function string.starts(String, Start)
  return string.sub(String, 1, string.len(Start)) == Start
end

-- recursively print tables 
-- for debugging, found on the internet
function print_r ( t )  
  local print_r_cache={}
  local function sub_print_r(t,indent)
    if (print_r_cache[tostring(t)]) then
      print(indent.."*"..tostring(t))
    else
      print_r_cache[tostring(t)]=true
      if (type(t)=="table") then
        for pos,val in pairs(t) do
          if (type(val)=="table") then
            print(indent.."["..pos.."] => "..tostring(t).." {")
            sub_print_r(val,indent..string.rep(" ",string.len(pos)+8))
            print(indent..string.rep(" ",string.len(pos)+6).."}")
          elseif (type(val)=="string") then
            print(indent.."["..pos..'] => "'..val..'"')
          else
            print(indent.."["..pos.."] => "..tostring(val))
          end
        end
      else
        print(indent..tostring(t))
      end
    end
  end
  if (type(t)=="table") then
    print(tostring(t).." {")
    sub_print_r(t,"  ")
    print("}")
  else
    sub_print_r(t,"  ")
  end
  print()
end
table.print = print_r


------------------------------------
-- Main
------------------------------------
 
number_of_nodes = 40
lines = read_lines_from_file("log.txt")
tuples = parse(lines)
aggregated_tuples  = aggregate(tuples)
save_file(aggregated_tuples, "aggregated_log.txt")

