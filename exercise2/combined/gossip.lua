-----------------------------------
-- GOSSIP-BASED DISSEMINATION
------------------------------------
-- The Anti-Entropy and Rumor-Mongering Dissemination Methods
-- For running on a Splay cluster
-- By Markus Roth
------------------------------------
-- I really wish this was Python
------------------------------------


------------------------------------
-- Test Setup Constants
------------------------------------

---START CONFIG SECTION---

-- is rumor mongering enabled?
do_rumor_mongering = true

-- is anti-entropy gossipping enabled?
do_anti_entropy = true

-- use the peer sampling service?
do_peer_sampling = true

-- gossip interval in seconds
gossip_interval = 5

-- gossip interval in seconds
peer_sampling_interval = 5

-- when i get infected, send this number of infection messages to random peers
-- only has effect when rumor mongering is active
initial_hops_to_live = 10

-- how many peers do I infect after being infected?
-- only has effect when rumor mongering is active
distribution_count = 10

-- the size of the local view for the peer sampling algorithm (parameter c)
peer_sampling_view_size = 10

-- how many peers are exchanged each round by peer sampling
peer_sampling_exchange_rate = 3

-- how many of the oldest peers should be skipped (parameter h)
peer_sampling_healer_parameter = 3

-- how many random items should be removed? (parameter s)
peer_sampling_shuffler_parameter = 3

-- peer selection policy for the peer sampling algorithm. Can be "rand" or "tail".
peer_selection_policy = "rand"

-- stop the simulation after this number of cycles
max_cycles = 20

-- start gossipping after this many cycles only, to give the peer sampling service time to work
-- only makes sense if peer sampling is eanbles
start_gossipping_after_cycles = 10

--END CONFIG SECTION---

------------------------------------
-- Debugging Constants
------------------------------------

-- print debug statements to std_out
print_debug_messages = false

-- print debug statements for just one node (set to 0 for printing for all nodes)
debug_for_node = 0

-- set this to true to not actually start the program, but run some tests 
unit_test_mode = false


------------------------------------
-- Initialization
------------------------------------

require("splay.base")
rpc = require("splay.urpc")
misc = require("splay.misc")

-- enable local testing
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
      local strT = date .. ' ' .. time .. ' ' .. position .. ' ' .. cycles .. ' ' .. info 
      print(strT)
    end
  end
else
  -- prepent cycle count to all messages
  logS = function (message) print(cycles .. " " .. message) end
end

rpc.server(job.me.port)


------------------------------------
-- Global Variables
------------------------------------

-- am I infected already?
infected = false

-- How many cycles have passed since the start of the simulation?
cycles = 0

-- am I currently waiting to send an infection message to my peers?
buffered = false

-- if I am currently waiting to send, what is the hops_to_live value of the message I am going to send?
buffered_hops_to_live = 0

-- the local view of the peer sampling service
-- items in it have the attributes "Age", "Peer", and "Id"
local_view = {}


------------------------------------
-- Peer Sampling Service
------------------------------------

-- the main peer sampling service loop
function peer_sampling_periodic()
  while true do
    events.sleep(peer_sampling_interval)
    -- local_view:lock()
    local peer = select_peer()
    local to_send = select_to_send()
    local received_peers = rpc.call(peer.peer, {'peer_sampling_receive', to_send, job.position})
    select_to_keep(received_peers)
    increase_local_view_age()
    -- local_view:unlock()
    local view_content_message = ("VIEW_CONTENT " .. job.position)
    for _, view_peer in pairs(local_view) do
      view_content_message = view_content_message .. " " .. tostring(view_peer.id)
    end
    print(view_content_message)
  end
end


-- increase the age of each element of the local view by one
function increase_local_view_age() 
  for _, peer in pairs(local_view) do
    peer.age = peer.age + 1
  end
end


function peer_sampling_receive(received_peers, sender_position)
  -- local_view:lock()
  local result = select_to_send()
  select_to_keep(received_peers)
  -- local_view:unlock()
  return result
end


function select_peer()
  if peer_selection_policy == "rand" then
    return random_peer_in_local_view()
  elseif peer_selection_policy == "tail" then
    return oldest_peer_in_local_view()
  end
end


function select_to_send()
  local to_send = {}

  --------------
  -- SHUFFLE --
  --------------

  local_view = misc.shuffle(local_view)
  debug_table("shuffled local_view", local_view)

  ------------------------
  -- MOVE OLDEST TO END --
  ------------------------

  -- sort a copy of local_view by age in reverse (oldest items first)
  local copy_of_local_view = {unpack(local_view)}
  table.sort(copy_of_local_view, function(peer1, peer2) return peer1.age > peer2.age end)
  debug_table("sorted copy of local_view", copy_of_local_view)
  
  -- the the oldest items that are now at the front of the copy
  local oldest_peers = {unpack(copy_of_local_view, 1, peer_sampling_healer_parameter)}
  debug_table("oldest peers", oldest_peers)

  -- iterate of the oldest_items, then remove each of the from local_view
  for _, oldest_peer in pairs(oldest_peers) do
      -- iterate over local view backwards so we can remove items and still use the same index for iterating
      for i = #local_view, 1, -1 do
        if peers_equal(oldest_peer.peer, local_view[i].peer) then 
          table.remove(local_view, i)
          debug("select_to_send removing element from local_view at index " .. i .. ". size now " .. #local_view)
        end
      end
  end

  -- add the oldest peers at the end of local_view
  for i = #oldest_peers, 1, -1 do
    local_view[#local_view + 1] = oldest_peers[i]
  end

  debug_table("local_view with oldest moved to end", local_view)

  --------------------
  -- SELECT TO SEND --
  --------------------

  -- to_send are the first items of local_view
  local to_send = {unpack(local_view, 1, peer_sampling_exchange_rate - 1)}
  debug_table("to_send before self added", to_send)

  -- add self to to_send with age 0 at the beginning of the list (index 1)
  table.insert(to_send, 1, {id = tostring(job.position), peer = job.me, age = 0})
  debug_table("to_send after self added", to_send)

  return to_send
end


function select_to_keep(received_peers)
  -----------
  -- MERGE --
  -----------

  -- merge the received peers with the local view
  -- but if the item is already present, don't add it again
  for _, peer in pairs(received_peers) do
    local already_present = false
    for _, local_peer in pairs(local_view) do
      if peers_equal(peer.peer, local_peer.peer) then
        already_present = true
      end
    end
    if not already_present then
      debug_table("inserting to local view", peer)

      -- if the new peer is not present, add the new item to the end of local_view
      peer.id = tonumber(peer.id)
      peer.age = tonumber(peer.age)
      table.insert(local_view, #local_view + 1, peer)
    end
  end

  debug_table("merged local_view with received_peers", local_view)
  
  ----------
  -- HEAL --
  ----------

  -- remove the oldest items from local_view without changing its order
  local oldest_items_to_remove = math.min(peer_sampling_healer_parameter, #local_view - peer_sampling_view_size)
  debug_table("number of old peers to remove", oldest_items_to_remove)

  -- reverse-sort a copy of the local view by age, so the first n elements are the oldest ones
  local copy_of_local_view = {unpack(local_view)}
  table.sort(copy_of_local_view, function(peer1, peer2) return peer1.age > peer2.age end)
  debug_table("sorted copy of local view (oldest first)", copy_of_local_view)

  -- get a list of the oldest peers. these need to be removed from the local view
  local oldest_peers = {}
  if oldest_items_to_remove > 0 then
    oldest_peers = {unpack(copy_of_local_view, 1, oldest_items_to_remove)}
  end
  debug_table("oldest peers to be removed", oldest_peers)

  -- iterate of the peers to remove, then remove each of the from local_view
  for _, old_peer in pairs(oldest_peers) do
      -- iterate over local view backwards so we can remove items and still use the same index for iterating
      for i = #local_view, 1, -1 do
        if peers_equal(old_peer.peer, local_view[i].peer) then 
          table.remove(local_view, i) 
          debug("select_to_keep 1 removing element from local_view at index " .. i .. ". size now " .. #local_view)
        end
      end
  end
  debug_table("local view with oldest removed", local_view)

  ---------------
  --- SHUFFLE ---
  ---------------

  -- remove the head items from local view
  local head_items_to_remove = math.min(peer_sampling_shuffler_parameter, #local_view - peer_sampling_view_size)
  debug_table("head items to remove", head_items_to_remove)


  if head_items_to_remove > 0 then
    debug_table("local_view before removing head items", local_view)
    local_view = {unpack(local_view, head_items_to_remove + 1, #local_view)}
    debug_table("local_view after removing head items", local_view)
  end

  --------------
  --- REDUCE ---
  --------------

  -- reduce size of local view to target size by removing random items
  local number_of_random_items_to_remove = math.max(0, #local_view - peer_sampling_view_size)
  debug_table("number of random items to remove", number_of_random_items_to_remove)

  -- create random copy of local_view and select top items to remove from local_view (so we get randomization without duplicates)
  local copy_of_local_view = {unpack(local_view)}
  copy_of_local_view = misc.shuffle(copy_of_local_view)

  local random_items_to_remove = {}
  if number_of_random_items_to_remove > 0 then
    random_items_to_remove = {unpack(copy_of_local_view, 1, number_of_random_items_to_remove)}
  end

  debug_table("items to remove", random_items_to_remove)

  for _, peer_to_remove in pairs(random_items_to_remove) do
    for i = #local_view, 1, -1 do
      debug_table("peer to remove", peer_to_remove)
      debug_table("local view item", local_view[i])
      if peers_equal(peer_to_remove.peer, local_view[i].peer) then 
        table.remove(local_view, i) 
        debug("select_to_keep 2 removing element from local_view. size now " .. #local_view)
      end
    end
  end
end


-- are two peers equal regarding to ip address and port?
function peers_equal(peer1, peer2)
  return peer1.ip == peer2.ip and peer1.port == peer2.port
end


function random_peer_in_local_view() 
  return local_view[math.random(#local_view)]
end

function oldest_peer_in_local_view()
  local oldest_age = 0
  local oldest_peer = {}
  for _, peer in pairs(local_view) do
    if peer.age < oldest_age then
      local oldest_peer = peer
      local oldest_age = peer.age
    end
  end
  return oldest_peer
end

function compare_by_age(peer1, peer2) 
  return peer1.age < peer2.age
end


------------------------------------
-- Anti-Entropy
------------------------------------

-- repeadedly find a random peer, and exchange the infected state with them
-- if either of the peers are infected before, both are infected after
function anti_entropy_periodic()
  local exchange_peer_position = find_random_peer()
  debug(job.position .. " <--anti-entropy--> " .. exchange_peer_position)
  local exchange_peer = job.nodes[exchange_peer_position]
  local peer_is_infected = rpc.call(exchange_peer, {'anti_entropy_infect', infected, job.position})

  -- if the peer is infected, but I am not yet infected, then I have now become infected
  if peer_is_infected and not infected then
    debug('Position ' .. job.position .. ' got anti-entropy infected by ' .. exchange_peer_position)
    logS("i_am_infected_by_anti_entropy")
    infected = true
  end
end


-- RPC call that gets called remotely by anti_entropy_periodic()
-- called to check if the node is infeced, and infects the node if the caller is infected
function anti_entropy_infect(caller_is_infected, sender_position)
  -- if the person calling me is infected, and I am not yet infected, then I am now infected
  if caller_is_infected and not infected then
    logS("i_am_infected_by_anti_entropy")
    debug('Position ' .. job.position .. ' got anti-entropy infected by ' .. sender_position)
    infected = true
  end
  return infected
end


-- select a random peer to exchange with
-- require that this peer is not oneself
function find_random_peer()
  if (do_peer_sampling) then
    return random_peer_in_local_view().id
  else 
    local exchange_peer_position = 0
    repeat
      local exchange_peer_position = math.random(#job.nodes)
    until exchange_peer_position ~= job.position
    return exchange_peer_position
  end
end


------------------------------------
-- Rumor Mongering
------------------------------------

-- repeatedly check if there is a message buffered
-- if there is, select a number of random exchange peers
-- and send them the message
function rumor_mongering_periodic()
  if buffered then
    local exchange_peer_positions = find_random_peers()
    for exchange_peer_position, _ in pairs(exchange_peer_positions) do
      local exchange_peer = job.nodes[exchange_peer_position]
      rpc.call(exchange_peer, {'rumor_mongering_infect', buffered_hops_to_live, job.position})
      debug(job.position .. " <--rumor-mongering--> " .. exchange_peer_position)
    end
    buffered = false
  end
end


-- RPC call that can be use to infect this node
-- if the node is not already infected, it becomes infected
-- if the node is not already buffering, it starts to buffer sending a message
-- if the node is already buffering, it takes the max hops_to_live from what it 
-- already has and what is gets from the current message
function rumor_mongering_infect(hops_to_live, sender_position)
  if not infected then
    logS("i_am_infected_by_rumor_mongering")
    debug('Position ' .. job.position .. ' got infected by ' .. sender_position)
    infected = true
  else
    debug('Position ' .. job.position .. ' received duplicate from ' .. sender_position)
    logS("duplicate_received")
  end
  if not buffered or buffered and hops_to_live - 1 > buffered_hops_to_live then
    local buffered_hops_to_live = hops_to_live - 1
    if buffered_hops_to_live > 0 then
      buffered = true
    end
  end
end
  

-- select a number random peers to exchange with
-- require that none of these peers is not oneself
-- and that every peer occurs only once
-- returns the index of the peer in job.nodes variable
function find_random_peers()

  -- if we use peer sampling, we copy the local view, randomzie it, and take the first distribution_count items
  if do_peer_sampling then
    local copy_of_local_view = {unpack(local_view)}
    copy_of_local_view = misc.shuffle(copy_of_local_view)
    local random_peers = {unpack(copy_of_local_view, 1, distribution_count)}
    local random_peer_ids = {}
    for _, peer in pairs(random_peers) do
      table.insert(random_peer_ids, peer.id)
    end
    return random_peer_ids

  -- otherwise, we take a random number of peers from job.nodes, requiring that we have no doubple items and don't shoose ourself
  else
    local exchange_peer_positions = {}
    local found_peers = 0
    repeat
      repeat
        local exchange_peer_position = math.random(#job.nodes)
      until (exchange_peer_position ~= job.position and (not exchange_peer_positions[exchange_peer_position]))
      exchange_peer_positions[exchange_peer_position] = true
      local found_peers = found_peers + 1
    until found_peers == distribution_count
    return exchange_peer_positions
  end
end


------------------------------------
-- Helper Functions 
------------------------------------

-- periodically update cycle count and start the periodic functions for the simulation
function cycle() 
  while true do
  
    -- wait for next cycle
    events.sleep(gossip_interval)

    -- increase cycle count 
    cycles = cycles + 1

    -- end the program after the maximum number of cyles
    if cycles >= max_cycles then
      print("FINAL: node " .. job.position .. " " .. tostring(infected))
      os.exit()
    end

    -- only start the dissemination exchanges after a certain amount of cyles
    -- so the peer sampling service has some time to work
    if cycles >= start_gossipping_after_cycles then

      -- call rumor mongering dissemination method
      if do_rumor_mongering then
        events.thread(rumor_mongering_periodic)
      end

      -- call anti entropy dissemination method
      if do_anti_entropy then
        events.thread(anti_entropy_periodic)
      end

    end

  end 
end


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
-- Tests
------------------------------------

function run_tests()
  -- always use the same random seed for tests for deterministic results
  math.randomseed(3)

  -- set parameters for test mode
  peer_sampling_view_size = 4
  peer_sampling_exchange_rate = 2
  peer_sampling_healer_parameter = 2
  peer_sampling_shuffler_parameter = 2
  peer_selection_policy = "tail"

  test_select_to_send_self_added()
  test_select_to_send_oldest_removed()
  test_select_to_send_healer_parameter()
  test_select_to_send_exchange_rate()

  test_select_to_keep_removed_duplicates()
  test_select_to_keep_append_received()
  test_select_to_keep_oldest_removed()
  test_select_to_keep_shuffle_parameter_respeced()



  -- select_to_send
  local_view = {}
  local_view[#local_view + 1] = {peer = { ip = "1", port = "1"}, age = 1, id = 1}
  local_view[#local_view + 1] = {peer = { ip = "2", port = "1"}, age = 2, id = 2}
  local_view[#local_view + 1] = {peer = { ip = "3", port = "1"}, age = 3, id = 3}
  local_view[#local_view + 1] = {peer = { ip = "4", port = "1"}, age = 4, id = 4}
  local_view[#local_view + 1] = {peer = { ip = "5", port = "1"}, age = 5, id = 5}

  local to_send = select_to_send()
  print("select_to_send results:")
  table.print(to_send)
  print("select_to_send local_view:")
  table.print(local_view)
  

  -- select_to_keep
  local_view = {}
  local_view[#local_view + 1] = {peer = { ip = "1", port = "1"}, age = 1, id = 1}
  local_view[#local_view + 1] = {peer = { ip = "2", port = "1"}, age = 2, id = 2}
  local_view[#local_view + 1] = {peer = { ip = "3", port = "1"}, age = 3, id = 3}

  local received = {}
  received[#received + 1] = {peer = { ip = "4", port = "1"}, age = 4, id = 4}
  received[#received + 1] = {peer = { ip = "5", port = "1"}, age = 5, id = 5}

  select_to_keep(received)

  print("local_view after select_to_keep:")
  table.print(local_view)

  os.exit()

end


function test_select_to_send_self_added()
  local_view = {}
  local to_send = select_to_send()
  assert(#to_send == 1, "to_send has more than one entry")
  assert(to_send[1].peer == job.me, "to_send does not contain self")
end


function test_select_to_send_oldest_removed()
  print("TEST: Select To Send: Oldest item removed")
  peer_sampling_view_size = 2
  peer_sampling_exchange_rate = 2
  peer_sampling_healer_parameter = 1
  peer_sampling_shuffler_parameter = 0

  local_view = {}
  local_view[#local_view + 1] = {peer = { ip = "2", port = "1"}, age = 2, id = 2}
  local_view[#local_view + 1] = {peer = { ip = "1", port = "1"}, age = 1, id = 1}
  job.me = { ip = "3", port = "1"}

  local to_send = select_to_send()
  debug_table("to send", to_send)
  debug_table("local view", local_view)
  assert(job.me == to_send[1].peer, "self not at start of to_send")
  assert(to_send[2].age == "1", "oldest item not removed")
end


function test_select_to_send_healer_parameter()
  print("TEST: Select To Send: Healer parameter respected")
  peer_sampling_view_size = 3
  peer_sampling_exchange_rate = 3
  peer_sampling_healer_parameter = 2
  peer_sampling_shuffler_parameter = 0

  local_view = {}
  local_view[#local_view + 1] = {peer = { ip = "1", port = "1"}, age = 2, id = 2}
  local_view[#local_view + 1] = {peer = { ip = "2", port = "1"}, age = 9, id = 2}
  local_view[#local_view + 1] = {peer = { ip = "3", port = "1"}, age = 7, id = 2}
  local_view[#local_view + 1] = {peer = { ip = "4", port = "1"}, age = 1, id = 3}
  local_view[#local_view + 1] = {peer = { ip = "5", port = "1"}, age = 3, id = 1}
  job.me = { ip = "0", port = "1"}

  local to_send = select_to_send()
  debug_table("to send", to_send)
  debug_table("local view", local_view)
  for _, ts in pairs(to_send) do
    print(ts.age)
    assert(ts.age < 7, "one of the oldest two items was to be sent")
  end
end


function test_select_to_send_exchange_rate()
  print("TEST: Select To Send: Exchange rate")
  peer_sampling_view_size = 3
  peer_sampling_exchange_rate = 1
  peer_sampling_healer_parameter = 2
  peer_sampling_shuffler_parameter = 0

  local_view = {}
  local_view[#local_view + 1] = {peer = { ip = "1", port = "1"}, age = 2, id = 2}
  job.me = { ip = "0", port = "1"}

  local to_send = select_to_send()
  debug_table("to send", to_send)
  debug_table("local view", local_view)
  assert(#to_send == 1, "too many exchanged for view exchange size")
end


function test_select_to_keep_removed_duplicates()
  local_view = {}
  local_view[#local_view + 1] = {peer = { ip = "1", port = "1"}, age = 1, id = 1}
  local received = {}
  received[#received + 1] = {peer = { ip = "1", port = "1"}, age = 4, id = 1}

  select_to_keep(received)

  debug_table("local view", local_view)
  assert(#local_view == 1, "duplicate not removed")
end


function test_select_to_keep_append_received()
  local_view = {}
  local_view[#local_view + 1] = {peer = { ip = "1", port = "1"}, age = 1, id = 1}
  local received = {}
  received[#received + 1] = {peer = { ip = "2", port = "1"}, age = 4, id = 2}

  select_to_keep(received)

  assert(#local_view == 2, "received peer not added")
end


function test_select_to_keep_oldest_removed()
  peer_sampling_view_size = 1
  local_view = {}
  local_view[#local_view + 1] = {peer = { ip = "1", port = "1"}, age = 6, id = 1}
  local received = {}
  received[#received + 1] = {peer = { ip = "2", port = "1"}, age = 1, id = 2}

  select_to_keep(received)

  assert(local_view[1].age == 1, "older peer was not replaced by received newer peer")
end


function test_select_to_keep_shuffle_parameter_respeced()
  peer_sampling_view_size = 2
  peer_sampling_shuffler_parameter = 2
  peer_sampling_healer_parameter = 0
  local_view = {}
  local_view[#local_view + 1] = {peer = { ip = "1", port = "1"}, age = 6, id = 1}
  local_view[#local_view + 1] = {peer = { ip = "3", port = "1"}, age = 6, id = 3}
  local_view[#local_view + 1] = {peer = { ip = "4", port = "1"}, age = 6, id = 4}
  local_view[#local_view + 1] = {peer = { ip = "5", port = "1"}, age = 6, id = 5}
  local received = {}
  received[#received + 1] = {peer = { ip = "2", port = "1"}, age = 1, id = 2}

  select_to_keep(received)

  debug_table("local_view", local_view)
  assert(#local_view ==  peer_sampling_view_size, "view size not correct")
  for _, peer in pairs(local_view) do
    assert(peer.id > "2", "shuffle parameter not respected, too few items removed")
  end
end

function assert(condition, message) 
  if not condition then
    print("ASSERTION ERROR!")
    print(message)
    os.exit(1)
  end
end

------------------------------------
-- Main 
------------------------------------


function main()
  -- init random number generator
  math.randomseed(job.position*os.time())

  -- initialize table printinf for debugging

  -- wait for all nodes to start up (conservative)
  events.sleep(2)

  if do_peer_sampling then
    -- create the local_view by:
    -- packing all peers into a list all_peers with additional metadata
    local all_peers = {}

    for index, node in ipairs(job.nodes) do
      -- exclude self
      if index ~= job.position then
        -- pack the nodes into the format used local_view with peer, age and id
        view_entry = {peer = node, age = 0, id = index}
        table.insert(all_peers, view_entry)
      end
    end

    -- shuffle the all_peers list
    all_peers = misc.shuffle(all_peers)

    -- take the first peer_sampling_view_size elements from the list and adding them to local_view
    for _, peer in pairs({unpack(all_peers, 1, peer_sampling_view_size)}) do
      debug_table("inserting to local view", peer)
      table.insert(local_view, peer)
    end

    -- start the thread that does the peer sampling service exchange
    events.thread(peer_sampling_periodic)
  end

  -- desynchronize the nodes
  local desync_wait = (gossip_interval * math.random())

  -- the first node is the source and is infected since the beginning
  if job.position == 1 then
    infected = true
    debug(job.position .. ' got infected as patient zero')
    logS("i_am_infected_as_patient_zero")
    desync_wait = 0
    buffered = true
    buffered_hops_to_live = initial_hops_to_live
  end

  debug("waiting for ".. desync_wait.. " to desynchronize")
  events.sleep(desync_wait)  

  events.thread(cycle)
end  


------------------------------------
-- Start
------------------------------------

if unit_test_mode then
  events.thread(run_tests)
else
  events.thread(main)  
end

events.loop()
