-----------------------------------
-- DISTRIBUTED HASH TABLE
------------------------------------
-- Key-based routing layer 
-- and distributed hash table on top
-- By Markus Roth
------------------------------------
-- LUA is starting to grow on me
------------------------------------

require("splay.base")
crypto = require("crypto")
rpc = require("splay.urpc")
misc = require("splay.misc")
require("debugger")

------------------------------------
-- Default values for Test Setup
------------------------------------

-- maximum seconds seconds to wait until joining the ring
-- a random value between 0 and this value is used
join_desync_max_interval = 10

-- issue search queries after waiting for all nodes to be in the ring
number_of_queries = 0

-- the size of node IDs, in bits
-- needs to be a multiple of 4 so the hashing function works as it does
-- (it cuts characters off a hex number string, representing 4 bits per character)
-- 32 = big
node_id_size = 4

-- if true, create finger tables
-- if false, nodes only are aware of their successor and predecessor
use_fingers = true

------------------------------------
-- Debugging Constants
------------------------------------


-- print debug statements to std_out
print_debug_messages = true

-- interval in seconds to show the ring string
ring_string_interval = 10

-- print debug statements for just one node (set to 0 for printing for all nodes)
debug_for_node = 0

-- set this to true to not actually start the program, but run some tests 
unit_test_mode = false


------------------------------------
-- Global Variables
------------------------------------

-- reference to own node
self_node = {}

-- the closest node with a smaller ID
predecessor = {}

-- the table of fingers for the local node
fingers = {}

-- a lock to prevent race conditions when getting neighbors
neighbor_lock = events.lock()

------------------------------------
-- Getters and Setters
------------------------------------

-- gets the node info of the predecessor to this node
function get_predecessor()
    return predecessor
end

-- sets the predecessor node for this node
function set_predecessor(new_value)
    predecessor = new_value
end

-- gets the successor node to this node
function get_successor()
    return fingers[1]
end

-- sets the predecessor node for this node
function set_successor(new_value)
    fingers[1] = new_value
end


------------------------------------
-- Key-based routing
------------------------------------

-- get a string representing the range i am responsible for
function my_range()
    if fingers and fingers[1] and fingers[1].ip then 
        local from = get_own_key()
        local to = normalize_id(get_id(fingers[1]))
        return "[" .. from .. ":" .. to .. "]"
    else
        return ""
    end
end

------------------------------------
-- Ring String
------------------------------------

-- repeadedly start a process that runs around the entire ring, picking up an additional entry on each hop
-- when the round robin calling gets back to the host node, it prints the ring topology to the log file
function print_ring()
    while true do
        events.sleep(ring_string_interval)  
        rpc.call(fingers[1], {"print_ring_rec", my_ring_string_entry(), self_node})
    end
end

-- the recursive function of the ring string: gets called from predecessor in ring, adds own substring, calls successor 
function print_ring_rec(ring_string, start_node)
    if self_node.port == start_node.port and self_node.ip == start_node.ip then
        logS("RING STRING:\n" .. ring_string)
    else
        ring_string = ring_string .. my_ring_string_entry()
        rpc.call(fingers[1], {"print_ring_rec", ring_string, start_node})
    end
end

-- generate own entry for the ring string
function my_ring_string_entry()
    finger_string = " "
    for i = 1, node_id_size do
        finger_string = finger_string .. normalize_id(get_id(fingers[i])) .. " "
    end
    return "ID " .. job.position .. " key: " .. get_own_key() .. " range: " .. my_range() .. " fingers: [" .. finger_string .. "]\n"
end


------------------------------------
-- Measuring Query Performance
------------------------------------

-- create a query for measuring query performance
-- generate a key and measure how many hops it takes to find its predecessor in the hash table
function query()
    for i = 1, number_of_queries do
        -- generate search key
        key_to_find = math.random(0, 2 ^ node_id_size)
        debug_print("finding key " .. key_to_find)

        -- if the search key is in the scope of the local node, return search distance of 0
        if is_in_range(key_to_find, self_node, fingers[1]) then
            logS("key_found " .. 0)
        else
            rpc.call(fingers[1], {"find_key", key_to_find, 0})
        end
    end
end

-- rpc for use by query()
-- checks if the receiveing node is responsible for the key.
-- if it is, it prints out the number of hops for parsing
-- if not, it forwards the key to the next node
function find_key(key_to_find, hops)
    debug_print("rpc finding key " .. key_to_find .. " hops " .. hops)
    hops = tonumber(hops) + 1
    if is_in_range(key_to_find, self_node, fingers[1]) then
        logS("key_found " .. hops)
    else
        rpc.call(fingers[1], {"find_key", key_to_find, hops})
    end
end


------------------------------------
-- Finding Neighbors
------------------------------------

-- find and return the node that is the fingers[1] of the passed key in the DHT ring
function find_successor(id)
    local pred = find_predecessor(id)
    local succ = rpc.call(pred, {'get_successor'})
    debug_print("ID " .. get_own_key() .. " says predecessor of " .. id .. " is " .. normalize_id(get_id(succ)))
    return succ
end

-- find the predecessor of the passed id
-- that is the node which is responsible for the id
-- where the passed id is between the the predecessor that is the response of this functino
-- and its fingers[1]
function find_predecessor(id)
    debug_print("ID " .. get_own_key() .. " finding closest predecessor of " .. id .. "...")
    local cursor = self_node
    local cursor_successor = fingers[1]
    while not (id > normalize_id(get_id(cursor)) and id <= normalize_id(get_id(cursor_successor))) do
        if use_fingers then
            -- if we use fingers, we can find our predecessor using the finger tables
            cursor = rpc.call(cursor, {"closest_preceeding_finger", id})
        else
            -- if not, we iterate around the ring step by step
            cursor = cursor_successor
        end
        cursor_predecessor = rpc.call(cursor, {"get_successor"})
    end
    return cursor
end

-- of all the local fingers, return the one that points the closest without going over to the passed id
function closest_preceeding_finger(id)
    for finger_number = node_id_size, 1, -1 do
        local finger_id = normalize_id(get_id(fingers[finger_number]))
        local condition = finger_id > get_own_key() and finger_id < tonumber(id) 
        if condition then 
            debug_print("ID " .. get_own_key() .. " says closest preceeding finger of " .. id .. " is " .. normalize_id(get_id(fingers[finger_number])))
            return fingers[finger_number]
        end
    end
    debug_print("ERROR: NO RETURN FROM PRECEEDING FINGER QUERY")
    print_finger_table()
    os.exit(1)
end


------------------------------------
-- Ring Initialization: No Fingers
------------------------------------

-- initialize predecessor and fingers[1] of own node
-- by recursively searching for own fingers[1], then finding its predecessor
-- and using those two nodes as own fingers[1] and predecessor
-- effectively getting in position between these two nodes
function init_neighbors(anchor_node)
    neighbor_lock:lock()

    fingers[1] = rpc.call(anchor_node, {"find_successor", get_own_key()})
    predecessor = rpc.call(fingers[1], {"get_predecessor"})

    -- set self as predecessor / fingers[1] on neighbors
    rpc.call(fingers[1], {'set_predecessor', self_node})
    rpc.call(predecessor, {'set_successor', self_node})

    debug_print(job.position .. ' joined the ring with successor ' .. fingers[1].port)
    debug_print(job.position .. ' joined the ring with predecessor ' .. predecessor.port)
    neighbor_lock:unlock()
end

------------------------------------
-- Ring Initialization: Fingers
------------------------------------

-- returns the starting key of the finger with the passed number
function get_finger_start(finger_number)
    return (get_own_key() + 2 ^ (finger_number - 1)) % (2 ^ node_id_size)
end

-- returns the key of the finger_number finger pointing at this node
function get_finger_source(finger_number)
    return (get_own_key() + 1 - 2 ^ (finger_number - 1)) % (2 ^ node_id_size)
end

-- initialize the own finger table when joining the ring
function init_finger_table(anchor_node)
    -- create the first finger entry in the finger table 
    -- the first finger is also the fingers[1] node
    fingers[1] = rpc.call(anchor_node, {"find_successor", get_finger_start(1)})
    predecessor = rpc.call(fingers[1], {"get_predecessor"})
    debug_print("ID " .. get_own_key() .. " setting finger 1 to ID " .. normalize_id(get_id(fingers[1])))

    -- create the rest of the finger table, finger by finger
    for finger_number = 2, node_id_size  do
        -- the starting key of the new finger
        local start = get_finger_start(finger_number)

        -- if the target key for the current finger is in the same range as the previous finger
        -- then the current finger targets the same node as the previous finger
        if is_in_range(start, self_node, fingers[finger_number - 1]) then
            debug_print("ID " .. get_own_key() .. " setting finger " .. finger_number .. " to copy of finger " .. finger_number - 1)
            fingers[finger_number] = fingers[finger_number - 1]
        else
            fingers[finger_number] = rpc.call(anchor_node, {"find_successor", start})
            debug_print("ID " .. get_own_key() .. " setting finger " .. finger_number .. " to ID " .. normalize_id(get_id(fingers[finger_number])))
        end
    end
end

-- update the fingers that point to this node when joining the ring
function update_others()
    debug_print("ID " .. get_own_key() .. " setting predecessor on ID " .. normalize_id(get_id(fingers[1])) .. " to self")
    rpc.call(fingers[1], {"set_predecessor", self_node})
    for finger_number = 1, node_id_size do
        -- calculate the source of the finger pointing at me, find its predecessor node
        -- and update the finger table on that node so the relevant finger points to me
        local finger_source = find_predecessor(get_finger_source(finger_number))
        debug_print("ID " .. get_own_key() .. " calling update finger table " .. normalize_id(get_id(finger_source)) .. " ")
        rpc.call(finger_source, {"update_finger_table", self_node, finger_number})
    end
end

-- update the finger table to point to a new value
function update_finger_table(target_node, finger_number) 
    -- if the current finger points to a node with the exact id that the finger should find the successor to
    -- we do not change the finger table, as it cannot get better
    local current_finger_id = normalize_id(get_id(fingers[finger_number]))
    local current_finger_start = get_finger_start(finger_number)
    local current_finger_is_perfect = current_finger_id == current_finger_start

    -- only update the finger table if the new finger is better
    -- better means it points to an id that is closer to the finger start than the existing finger
    local target_node_id = normalize_id(get_id(target_node))
    local new_finger_is_better = target_node_id >= current_finger_start and target_node_id < current_finger_id

    if not current_finger_is_perfect and new_finger_is_better then
        fingers[finger_number] = target_node
        debug_print("ID " .. get_own_key() .. " setting finger " .. finger_number .. " to ID " .. normalize_id(get_id(fingers[finger_number])))

        -- update the fingers on the predecessor node as well
        rpc.call(predecessor, {"update_finger_table", target_node, finger_number})
    end
end


------------------------------------
-- Hashing and Ranges
------------------------------------

-- test if the id is in the range between the two peers
-- peer2 might have a lower id than peer 1, so we need to wrap around
function is_in_range(id, peer1, peer2)
    local first_point = normalize_id(get_id(peer1)) 
    local second_point = normalize_id(get_id(peer2))
    return is_in_range_numeric(id, first_point, second_point)
end

function is_in_range_numeric(id, first_point, second_point)
    local result = false
    if second_point == first_point then
        -- if the second point is the first point, the node is its own successor
        -- this means there is only one node, and any point is within its range
        result = true 
    elseif second_point > first_point then
        -- if the second point is higher than the first point, there is no wrap around
        -- so we can compare simply
        result = id > first_point and id <= second_point
    elseif second_point < first_point then
        -- here we have a wraparound around the 0 point
        result = id > first_point or id <= second_point
    end
    return result
end

-- returns the id value for a given nodes ip address and port
-- using a hash function
function get_id(node)
    local id = compute_hash(node.ip .. node.port)
    return id
end

-- the ids need to wrap around the ring, so we need to take the modulo of them
-- the maximum id size is 2 to the pwoer of node_id_size
function normalize_id(id)
    local max_id = 2 ^ node_id_size
    return (id + 1) % max_id
end

-- compute a hash of a string, then truncate it to the first node_id_size bits
-- the hash is a hexadecimal string
-- so if the hash is ABCD
-- and node_id_size is 8 bits
-- then the hash ABCD is truncated to AB
-- which contains exactly 8 bits of information
-- this is then converted to a numerical value for returning
function compute_hash(value)
    local hash = crypto.evp.new("sha1"):digest(value)
    local shortened_hash = string.sub(hash, 1, node_id_size / 4)
    local shortened_hash_as_number = tonumber(shortened_hash, 16)
    return shortened_hash_as_number
end

-- calulcates the hash of own port and ip, gets the id from that and normalizes it
function get_own_key()
    return normalize_id(get_id(self_node))
end



------------------------------------
-- Debugging
------------------------------------

function print_finger_table()
    local own_id = get_own_key()
    local output_string = "FINGER TABLE FOR NODE " .. own_id .. "\n"
    for finger_number = 1, node_id_size do
        finger = fingers[finger_number]
        output_string = output_string .. finger_number .. ": " 
        output_string = output_string .. " id: " .. normalize_id(get_id(finger))
        output_string = output_string .. " start: " .. get_finger_start(finger_number)
        output_string = output_string .. " source: " .. get_finger_source(finger_number)
        output_string = output_string .. "\n"
    end
    output_string = output_string .. "\n"
    debug_print(output_string)
end

-- print debug message if debug flag is set
function debug_print(message)
  if print_debug_messages and (debug_for_node == 0 or debug_for_node == job.position) then
    logS(message)
  end
end

-- print debug message and table if debug flag is set
function debug_table(title, table_to_print)
  if print_debug_messages and (debug_for_node == 0 or debug_for_node == job.position) then
    logS(" ")
    logS(title)
    table.print(table_to_print)
    logS(" ")
  end
end

-- recursively print tables 
-- for debugging, found on the internet
function print_r(t)  
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

-- enable local testing
function enable_local_testing() 
    if not job then
      local utils = require("splay.utils")
      if #arg < 2 then  
        print("lua "..arg[0].." my_position nb_nodes")  
        os.exit()  
      else  
        local pos, total = tonumber(arg[1]), tonumber(arg[2])  
        job = utils.generate_job(pos, total, 20001)
        logS = function (info)
          local ts = os.date('*t', os.time())
          local date = ts.year .. '-' .. ts.month .. '-' .. ts.day
          local time = ts.hour .. ':' .. ts.min .. ':' .. ts.sec .. '.0'
          local position = '(' .. job.position .. ')'
          local range = my_range()
          local strT = date .. ' ' .. time .. ' ' .. position .. ' ' .. range .. ' ' .. info 
          print(strT)
        end
      end
    end
end

function main()
    debug_print("Starting node " .. job.position)
    -- init random number generator
    math.randomseed(job.position * os.time())

    -- desynchronize the nodes
    local desync_wait = join_desync_max_interval * math.random()

    -- set self_node to self for easier access
    self_node = job.me

    -- all nodes except first need to wait for everything to be ready
    if job.position ~= 1 then
        events.sleep(desync_wait)  
    end
    
    setup_links()

    -- the first nodes needs to start the string ring thread that regularly prints the ring status for debugging
    if job.position == 1 then
        debug_print(job.position .. ' joins the ring as node ' .. get_own_key())
        logS("join_as_first_node")
        events.thread(print_ring)  
    end

    -- we need to wait until the entire ring is completed before we start to launch queries
    -- otherwise the needed number of hops will be too low because the number of total nodes is lower
    events.sleep(join_desync_max_interval - desync_wait + 1)  
    events.thread(query)
end  

function setup_links()
    if use_fingers and job.position == 1 then
        -- set all fingers to point to self
        for finger_number = 1, node_id_size do
            fingers[finger_number] = self_node
        end
        fingers[1] = self_node
        predecessor = self_node

    -- fingers, all other nodes
    elseif use_fingers and job.position ~= 1 then
        init_finger_table(job.nodes[1])
        predecessor = rpc.call(fingers[1], {"get_predecessor"})
        update_others()

    -- no fingers and first node
    elseif not use_fingers and job.position == 1 then
        -- set up predecessor and fingers[1] to point to self
        fingers[1] = self_node
        predecessor = self_node

    -- no fingers and not first node
    elseif not use_fingers and job.position ~= 1 then
        init_neighbors(job.nodes[1])
        logS("join_ring")
    end
end

------------------------------------
-- Tests
------------------------------------

function run_tests()
    -- always print debug messages in test mode
    print_debug_messages = true

    debug_print("UNIT TEST MODE")
    -- always use the same random seed for tests for deterministic results
    math.randomseed(3)

    -- set parameters for test mode
    node_id_size = 8 -- 256 nodes

    self_node = {ip = "1", port = "1"} -- gives own ID of 24

    -- run tests
    test_compute_hash()
    test_normalize_id()
    test_is_in_range()
    test_get_finger_start()
    test_get_finger_source()

    -- exit program
    debug_print("UNIT TESTS COMPLETE")
    os.exit()
end


function test_compute_hash()
    local value = "Test-Value"
    local hash = compute_hash(value)
    assert(hash == 95, "hash must be correct")
end

function test_normalize_id()
    local id = 254
    local normalized_id = normalize_id(id)
    assert(normalized_id == 255, "normalize id does not ignore small enough values, instead of 255 is " .. normalized_id)

    id = 255
    normalized_id = normalize_id(id)
    assert(normalized_id == 0, "normalize id does not wrap around to 0")

    id = 256
    normalized_id = normalize_id(id)
    assert(normalized_id == 1, "normalize id does not wrap around")
end

function test_is_in_range()
    a = {ip = "1", port = "1"} --24
    b = {ip = "1", port = "2"} --124
    assert(is_in_range(50, a, b) == true, "50 between 24 and 124")
    assert(is_in_range(50, b, a) == false, "50 not between 124 and 24")
    assert(is_in_range(5, b, a) == true, "5 between 124 and 24")
    assert(is_in_range(130, b, a) == true , "130 between 124 and 24")
    assert(is_in_range(130, a, b) == false, "130 not between 24 and 124")
end

function test_get_finger_start()
    assert(get_finger_start(1) == 25, "finger 1 needs to be successor")
    assert(get_finger_start(2) == 26, "finger 2")
    assert(get_finger_start(3) == 28, "finger 3")
    assert(get_finger_start(4) == 32, "finger 4")
    assert(get_finger_start(8) == 152, "finger 8")
end

function test_get_finger_source()
    assert(get_finger_source(1) == 24, "finger 1 source")
    assert(get_finger_source(8) == 153, "finger 8 source")
end

function assert(condition, message) 
  if not condition then
    print("ASSERTION ERROR!")
    print(message)
    os.exit(1)
  end
end


------------------------------------
-- Start
------------------------------------

enable_local_testing()

if unit_test_mode then
    events.thread(run_tests)
else
    rpc.server(job.me.port)
    events.thread(main)  
end
events.run()
