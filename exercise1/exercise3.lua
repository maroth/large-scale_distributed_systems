function file_exists(file) 
    local f = io.open(file, "rb")
    if f then
        f:close() 
    end
    return f ~= nil
end

function lines_from(file)
    if not file_exists(file) then 
        return {} 
    end
    lines = {}
    for line in io.lines(file) do 
        lines[#lines + 1] = line
    end
    return lines
end

function tuples_from_lines(lines)
    tuples = {}
    fields = {"Timestamp", "Node", "Cpu1", "Cpu5", "Cpu15"}
    for i, line in ipairs(lines) do
        tuple = {}
        count = 1
        for element in string.gmatch(line, "%S+") do
            if fields[count] == "Node" then
                element = string.sub(element, 2, -2)
            end
            tuple[fields[count]] = element
            count = count + 1
        end
        tuples[i] = tuple
    end
    return tuples
end

function split_by_node(tuples)
    lists = {}
    for i, tuple in ipairs(tuples) do
        node = tuple["Node"]
        if lists[node] == nil then
            list = {tuple}
            lists[node] = list
        else
            table.insert(lists[node], tuple)
        end
    end
    return lists
end

function save_files(split_tuples)
    for node, tuples_of_node in pairs(split_tuples) do
        filename = node .. ".log"
        file = io.open(filename, "a")
        for j, single_tuple in ipairs(tuples_of_node) do
            timestamp = single_tuple["Timestamp"]
            cpu1 = single_tuple["Cpu1"]
            line = timestamp .. " " .. cpu1 .. "\n"
            file:write(line)
        end
        file:close()
    end
end

lines = lines_from("log.txt")
tuples = tuples_from_lines(lines)
split_tuples = split_by_node(tuples)
save_files(split_tuples)

