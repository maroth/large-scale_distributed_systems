------------------------------------
-- GOSSIP-BASED DISSEMINATION
------------------------------------
-- The Anti-Entropy and Rumor-Mongering Dissemination Methods
-- For running on a Splay cluster
-- By Markus Roth
------------------------------------
-- I really wish this was Python
------------------------------------


-- lock local_view with l:lock() l:unlock()


------------------------------------
-- Constants
------------------------------------

-- is rumor mongering enabled?
do_rumor_mongering = true

-- is anti-entropy gossipping enabled?
do_anti_entropy = true

-- gossip interval in seconds
gossip_interval = 5  

-- gossip interval in seconds
peer_sampling_interval = 5  

-- when i get infected, send this number of infection messages to random peers
-- only has effect when rumor mongering is active
initial_hops_to_live = 5

-- how many peers do I infect after being infected?
-- only has effect when rumor mongering is active
distribution_count = 3

-- the size of the local view for the peer sampling algorithm (parameter c)
peer_sampling_view_size = 5

-- how many peers are exchanged each round by peer sampling
peer_sampling_exchange_rate = 3

-- how many of the oldest peers should be skipped (parameter h)
peer_sampling_healer_parameter = 2

-- how many
peer_sampling_shuffler_parameter = 2

-- peer selection policy for the peer sampling algorithm. Can be "rand" or "tail".
peer_selection_policy = "rand"

-- stop the simulation after this number of cycles
max_cycles = 24

-- print debug statements to std_out
print_debug_messages = false


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
    local peer = select_peer()
    local to_send = select_to_send()
    local received_peers = rpc.call(peer, {'peer_sampling_receive', to_send, job.position})
    local_view = select_to_keep(received_peers)
    increate_local_view_age()
  end
end


-- increase the age of each element of the local view by one
function increase_local_view_age() 
  for _, peer in pairs(local_view) do
    peer["Age"] = peer["Age"] + 1
  end
end


function peer_sampling_receive(received_peers, sender_position)
  debug(job.position .. " received peers from " .. sender_position)
  local result = select_to_send(local_view)
  local_view = select_to_keep(received_peers)
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

  -- sort local_view so we can easily discard the last h elements
  table.sort(local_view, compare_by_age)

  -- once local_view is sorted by age, we can just take all elements except for the last h
  -- that measn the items from index 1 to index (size of local_view - h)
  to_send = table.unpack(local_view, 1, #local_view - peer_sampling_healer_parameter)

  -- add self to to_send and shuffle it
  misc.shuffle(to_send)
  table.insert(to_send, {job.me, 0})

  return to_send
end


function select_to_keep(received_peers)
  -- merge the received peers with the local view
  -- but if the item is already present, don't add it again
  for _, peer in received_peers do
    local already_present = false
    for _, local_peer in local_view do
      if peers_equal(peer["Peer"], local_peer["Peer"]) then
        already_present = true
      end
    end
    if not already_present then
      table.insert(local_view, peer)
    end
  end
  
  -- remove the oldest items from local_view without changing its order
  -- I assume this is important, because we need to first delete the oldest items, then the items in the head of the list
  local oldest_items_to_remove = math.min(peer_sampling_healer_parameter, #local_view - peer_sampling_view_size)

  -- reverse-sort a copy of the local view by age, so the first n elements are the oldest ones
  local copy_of_local_view = {unpack(local_view)}
  table.sort(copy_of_local_view, function(peer1, peer2) return peer1.age > peer2.age end)

  -- get a list of the oldest peers. these need to be removed from the local view
  local oldest_peers = {unpack(copy_of_local_view, 1, oldest_items_to_remove)}

  -- iterate of the peers to remove, then remove each of the from local_view
  for _, old_peer in oldest_peers do
      -- iterate over local view backwards so we can remove items and still use the same index for iterating
      for i = #local_view, 1, -1 do
        if peers_equal(old_peer, peer) then table.remove(local_view, i)
      end
  end

  -- remove the head items from local view
  local head_items_to_remove = math.min(peer_sampling_shuffler_parameter, #local_view - peer_sampling_view_size)
  local_view = {unpack(local_view, 1, #local_view - head_items_to_remove)}

  -- reduce size of local view to target size by removing random items
  local random_items_to_remove = math.max(0, #local_view - peer_sampling_view_size)
  misc.shuffle(local_view)
  local_view = {unpack(local_view, 1, #local_view - random_items_to_remove)}
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
    if peer["Age"] < oldest_age then
      oldest_peer = peer
      oldest_age = peer["Age"]
    end
  end
  return oldest_peer
end

function compare_by_age(peer1, peer2) 
  return peer1["Age"] < peer2["Age"] 
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
  local exchange_peer_position = 0
  repeat
    exchange_peer_position = math.random(#job.nodes)
  until exchange_peer_position ~= job.position
  return exchange_peer_position
end


------------------------------------
-- Rumor Mongering
------------------------------------

-- repeatedly check if there is a message buffered
-- if there is, select a number of random exchange peers
-- and send them the message
function rumor_mongering_periodic()
  if buffered then
    exchange_peer_positions = find_random_peers()
    for exchange_peer_position, _ in pairs(exchange_peer_positions) do
      exchange_peer = job.nodes[exchange_peer_position]
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
    buffered_hops_to_live = hops_to_live - 1
    if buffered_hops_to_live > 0 then
      buffered = true
    end
  end
end
  

-- select a number random peers to exchange with
-- require that none of these peers is not oneself
-- and that every peer occurs only once
-- P.S. LUA is not a very expressive language... This would be one line in Python
function find_random_peers()
  local exchange_peer_positions = {}
  local found_peers = 0
  repeat
    repeat
      exchange_peer_position = math.random(#job.nodes)
    until (exchange_peer_position ~= job.position and (not exchange_peer_positions[exchange_peer_position]))
    exchange_peer_positions[exchange_peer_position] = true
    found_peers = found_peers + 1
  until found_peers == distribution_count
  return exchange_peer_positions
end


------------------------------------
-- Helper Functions 
------------------------------------

-- periodically update cycle count and start the periodic functions for the simulation
function cycle() 
  while true do
  
    -- end the program after the maximum number of cyles
    if cycles >= max_cycles then
      debug("FINAL: node " .. job.position .. " " .. tostring(infected))
      os.exit()
    end

    -- wait for next cycle
    events.sleep(gossip_interval)

    -- increase cycle count 
    cycles = cycles + 1

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

-- print debug message if debug flag is set
function debug(message)
  if print_debug_messages then
    logS(message)
  end
end


------------------------------------
-- Main 
------------------------------------

function main()
  -- init random number generator
  math.randomseed(job.position*os.time())

  -- wait for all nodes to start up (conservative)
  events.sleep(2)

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

events.thread(main)  
events.loop()

------------------------------------
-- GOSSIP-BASED DISSEMINATION
------------------------------------
-- The Anti-Entropy and Rumor-Mongering Dissemination Methods
-- For running on a Splay cluster
-- By Markus Roth
------------------------------------
-- I really wish this was Python
------------------------------------


-- plot cycles not seconds
-- lock local_view with l:lock() l:unlock()


------------------------------------
-- Constants
------------------------------------

-- is rumor mongering enabled?
do_rumor_mongering = true

-- is anti-entropy gossipping enabled?
do_anti_entropy = true

-- gossip interval in seconds
gossip_interval = 5  

-- gossip interval in seconds
peer_sampling_interval = 5  

-- when i get infected, send this number of infection messages to random peers
-- only has effect when rumor mongering is active
initial_hops_to_live = 5

-- how many peers do I infect after being infected?
-- only has effect when rumor mongering is active
distribution_count = 3

-- the size of the local view for the peer sampling algorithm (parameter c)
peer_sampling_view_size = 5

-- how many peers are exchanged each round by peer sampling
peer_sampling_exchange_rate = 3

-- how many of the oldest peers should be skipped (parameter h)
peer_sampling_healer_parameter = 2

-- how many
peer_sampling_shuffler_parameter = 2

-- peer selection policy for the peer sampling algorithm. Can be "rand" or "tail".
peer_selection_policy = "rand"

-- stop the simulation after this number of cycles
max_cycles = 24

-- print debug statements to std_out
print_debug_messages = false


------------------------------------
-- Initialization
------------------------------------

require("splay.base")
rpc = require("splay.urpc")

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
  logS = function (message) print(cycles .. '  ' .. message) end
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
    local peer = select_peer()
    local to_send = select_to_send()
    local received_peers = rpc.call(peer, {'peer_sampling_receive', to_send, job.position})
    local_view = select_to_keep(received_peers)
    increate_local_view_age()
  end
end


-- increase the age of each element of the local view by one
function increase_local_view_age() 
  for _, peer in pairs(local_view) do
    peer["Age"] = peer["Age"] + 1
  end
end


function peer_sampling_receive(received_peers, sender_position)
  debug(job.position .. " received peers from " .. sender_position)
  local result = select_to_send(local_view)
  local_view = select_to_keep(received_peers)
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

  -- sort local_view so we can easily discard the last h elements
  table.sort(local_view, compare_by_age)

  -- once local_view is sorted by age, we can just take all elements except for the last h
  -- that measn the items from index 1 to index (size of local_view - h)
  to_send = table.unpack(local_view, 1, #local_view - peer_sampling_healer_parameter)

  -- add self to to_send and shuffle it
  misc.shuffle(to_send)
  table.insert(to_send, {job.me, 0})

  return to_send
end


function select_to_keep(received_peers)
  -- merge the received peers with the local view
  -- but if the item is already present, don't add it again
  for _, peer in received_peers do
    local already_present = false
    for _, local_peer in local_view do
      if peers_equal(peer["Peer"], local_peer["Peer"]) then
        already_present = true
      end
    end
    if not already_present then
      table.insert(local_view, peer)
    end
  end

  -- remove the oldest items from local view
  local items_to_remove = math.min(peer_sampling_healer_parameter, #local_view - peer_sampling_view_size)
  --TODO

  -- remove the head items from local view
  items_to_remove = math.min(peer_sampling_shuffler_parameter, #local_view - peer_sampling_view_size)
  --TODO

  -- reduce size of local view to target size by removing random items
  items_to_remove = math.max(0, #local_view - peer_sampling_view_size)
  --TODO
end


-- are two peers equal regarding to ip address and port?
function peers_equal(peer1, peer2)
  --TODO
end


function random_peer_in_local_view() 
  return local_view[math.random(#local_view)]
end

function oldest_peer_in_local_view()
  local oldest_age = 0
  local oldest_peer = {}
  for _, peer in pairs(local_view) do
    if peer["Age"] < oldest_age then
      oldest_peer = peer
      oldest_age = peer["Age"]
    end
  end
  return oldest_peer
end

function compare_by_age(peer1, peer2) 
  return peer1["Age"] < peer2["Age"] 
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
  local exchange_peer_position = 0
  repeat
    exchange_peer_position = math.random(#job.nodes)
  until exchange_peer_position ~= job.position
  return exchange_peer_position
end


------------------------------------
-- Rumor Mongering
------------------------------------

-- repeatedly check if there is a message buffered
-- if there is, select a number of random exchange peers
-- and send them the message
function rumor_mongering_periodic()
  if buffered then
    exchange_peer_positions = find_random_peers()
    for exchange_peer_position, _ in pairs(exchange_peer_positions) do
      exchange_peer = job.nodes[exchange_peer_position]
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
    buffered_hops_to_live = hops_to_live - 1
    if buffered_hops_to_live > 0 then
      buffered = true
    end
  end
end
  

-- select a number random peers to exchange with
-- require that none of these peers is not oneself
-- and that every peer occurs only once
-- P.S. LUA is not a very expressive language... This would be one line in Python
function find_random_peers()
  local exchange_peer_positions = {}
  local found_peers = 0
  repeat
    repeat
      exchange_peer_position = math.random(#job.nodes)
    until (exchange_peer_position ~= job.position and (not exchange_peer_positions[exchange_peer_position]))
    exchange_peer_positions[exchange_peer_position] = true
    found_peers = found_peers + 1
  until found_peers == distribution_count
  return exchange_peer_positions
end


------------------------------------
-- Helper Functions 
------------------------------------

-- periodically update cycle count and start the periodic functions for the simulation
function cycle() 
  while true do
  
    -- end the program after the maximum number of cyles
    if cycles >= max_cycles then
      debug("FINAL: node " .. job.position .. " " .. tostring(infected))
      os.exit()
    end

    -- wait for next cycle
    events.sleep(gossip_interval)

    -- increase cycle count 
    cycles = cycles + 1

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


-- print debug output if debug flag is enables
function debug(message) 
  if print_debug_messages then logS(message) end
end


------------------------------------
-- Main 
------------------------------------

function main()
  -- init random number generator
  math.randomseed(job.position*os.time())

  -- wait for all nodes to start up (conservative)
  events.sleep(2)

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

events.thread(main)  
events.loop()

