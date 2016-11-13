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

------------------------------------
-- Default values for Test Setup
------------------------------------

-- maximum seconds seconds to wait until joining the ring
-- a random value between 0 and this value is used
join_desync_max_interval = 100

-- the size of node IDs, in bits
node_id_size = 32

------------------------------------
-- Debugging Constants
------------------------------------

-- print debug statements to std_out
print_debug_messages = true

-- print debug statements for just one node (set to 0 for printing for all nodes)
debug_for_node = 0

-- set this to true to not actually start the program, but run some tests 
unit_test_mode = false

------------------------------------
-- Initialization
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


------------------------------------
-- Global Variables
------------------------------------

-- reference to own node
self_node = {}

-- the closest node with a bigger ID
successor = {}

-- the closest node with a smaller ID
predecessor = {}

-- a lock to prevent race conditions when getting neighbors
neighbor_lock = events.lock()

------------------------------------
-- RPC calls
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
    return successor
end

-- sets the predecessor node for this node
function set_successor(new_value)
    successor = new_value
end


------------------------------------
-- Key-based routing
------------------------------------

-- get a string representing the range i am responsible for
function my_range()
    if successor.ip then 
        local from = normalize_id(get_id(self_node))
        local to = normalize_id(get_id(successor))
        return "[" .. from .. ":" .. to .. "]"
    else
        return ""
    end
end

function my_ring_string_entry()
    return job.position .. " " .. normalize_id(get_id(self_node)) .. " " .. my_range() .. "\n"
end

function print_ring()
    while true do
        events.sleep(2)  
        rpc.call(successor, {"print_ring_rec", my_ring_string_entry(), self_node})
    end
end

function print_ring_rec(ring_string, start_node)
    if self_node.port == start_node.port and self_node.ip == start_node.ip then
        logS("RING STRING:\n" .. ring_string)
    else
        ring_string = ring_string .. my_ring_string_entry()
        rpc.call(successor, {"print_ring_rec", ring_string, start_node})
    end
end

-- generate a key and measure how many hops it takes to find its predecessor in the hash table
function query()
    for i = 1, 500 do
        -- generate search key
        key_to_find = math.random(0, 2 ^ node_id_size)
        debug("finding key " .. key_to_find)

        -- if the search key is in the scope of the local node, return search distance of 0
        if is_in_range(key_to_find, self_node, successor) then
            logS("key_found " .. 0)
        else
            rpc.call(successor, {"find_key", key_to_find, 0})
        end
    end
end

-- rpc for use by query()
-- checks if the receiveing node is responsible for the key.
-- if it is, it prints out the number of hops for parsing
-- if not, it forwards the key to the next node
function find_key(key_to_find, hops)
    debug("rpc finding key " .. key_to_find .. " hops " .. hops)
    hops = tonumber(hops) + 1
    if is_in_range(key_to_find, self_node, successor) then
        logS("key_found " .. hops)
    else
        rpc.call(successor, {"find_key", key_to_find, hops})
    end
end

-- find and return the node that is the successor of the passed key in the DHT ring
function find_successor(id)
    local pred = find_predecessor(id)
    local succ = rpc.call(pred, {'get_successor'})
    return succ
end

-- find the predecessor of the passed id
-- that is the node which is responsible for the id
-- where the passed id is between the the predecessor that is the response of this functino
-- and its successor
function find_predecessor(id)
    local cursor = self_node
    local cursor_successor = successor
    while not is_in_range(id, cursor, cursor_successor) do
        cursor = cursor_successor
        cursor_successor = rpc.call(cursor, {'get_successor'})
    end
    return cursor
end

-- initialize predecessor and successor of own node
-- by recursively searching for own successor, then finding its predecessor
-- and using those two nodes as own successor and predecessor
-- effectively getting in position between these two nodes
function init_neighbors(anchor_node)
    neighbor_lock:lock()
    -- local id is hash of port and ip
    local id = get_id(self_node)
    -- normalize to wrap around 0
    local normalized_own_id = normalize_id(id)

    successor = rpc.call(anchor_node, {"find_successor", normalized_own_id})
    predecessor = rpc.call(successor, {"get_predecessor"})

    -- set self as predecessor / successor on neighbors
    rpc.call(successor, {'set_predecessor', self_node})
    rpc.call(predecessor, {'set_successor', self_node})

    debug(job.position .. ' joined the ring with successor ' .. successor.port)
    debug(job.position .. ' joined the ring with predecessor ' .. predecessor.port)
    neighbor_lock:unlock()
end

-- test if the id is in the range between the two peers
-- peer2 might have a lower id than peer 1, so we need to wrap around
function is_in_range(id, peer1, peer2)
    local first_point = normalize_id(get_id(peer1)) 
    local second_point = normalize_id(get_id(peer2))
    local result = false
    if second_point == first_point then
        -- if the second point is the first point, the node is its own successor. 
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


------------------------------------
-- DEBUG
------------------------------------

-- print debug message if debug flag is set
function debug(message)
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

function main()
    debug("Starting node " .. job.position)
    -- init random number generator
    math.randomseed(job.position * os.time())

    -- desynchronize the nodes
    local desync_wait = (join_desync_max_interval * math.random())

    -- set self_node to self for easier access
    self_node = job.me

    -- the first node is the source and is infected since the beginning
    if job.position == 1 then
        -- if we are node 1, we start as our own ring, with ourselves as neighbors
        successor = self_node
        predecessor = self_node

        debug(job.position .. ' joins the ring as node 1')
        logS("join_as_first_node")
        events.thread(print_ring)  
    else
        debug("waiting for ".. desync_wait.. " to desynchronize")
        events.sleep(desync_wait)  
        init_neighbors(job.nodes[1])
        logS("join_ring")
    end

    -- we need to wait until the entire ring is completed before we start to launch queries
    -- otherwise the needed number of hops will be too low because the number of total nodes is lower
    events.sleep(join_desync_max_interval * 2)  
    events.thread(query)
end  

------------------------------------
-- Tests
------------------------------------

function run_tests()
    -- always print debug messages in test mode
    print_debug_messages = true

    debug("UNIT TEST MODE")
    -- always use the same random seed for tests for deterministic results
    math.randomseed(3)

    -- set parameters for test mode
    node_id_size = 8 -- 256 nodes

    -- run tests
    test_compute_hash()
    test_normalize_id()
    test_is_in_range()

    -- exit program
    debug("UNIT TESTS COMPLETE")
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
