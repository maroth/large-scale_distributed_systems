------------------------------------
-- GOSSIP-BASED DISSEMINATION
------------------------------------
-- The Anti-Entropy and Rumor-Mongering Dissemination Methods
-- For running on a Splay cluster
-- By Markus Roth
------------------------------------
-- I wish this was Python
------------------------------------


------------------------------------
-- Constants
------------------------------------

-- is rumor mongering enabled?
do_rumor_mongering = true

-- is anti-entropy gossipping enabled?
do_anti_entropy = true

-- gossip interval in seconds
gossip_interval = 5  

-- when i get infected, send this number of infection messages to random partners
-- only has effect when rumor mongering is active
initial_hops_to_live = 5

-- how many partners do I infect after being infected?
-- only has effect when rumor mongering is active
distribution_count = 3

-- total running time
max_time = 120 

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
      local strT = date .. ' ' .. time .. ' ' .. position .. ' ' .. info
      print(strT)
    end
  end
else
  logS = print
end

rpc.server(job.me.port)


------------------------------------
-- variables
------------------------------------

-- am I infected already?
infected = false

-- am I currently waiting to send an infection message to my peers?
buffered = false

-- if I am currently waiting to send, what is the hops_to_live value of the message I am going to send?
buffered_hops_to_live = 0


------------------------------------
-- helper functions 
------------------------------------

-- print debug message if debug flag is set
function debug(message)
  if print_debug_messages then
    logS(message)
  end
end


-- terminate the program after a given amount of time
function terminator()
  events.sleep(max_time)
  debug("FINAL: node " .. job.position .. " " .. tostring(infected))
  os.exit()
end


------------------------------------
-- anti entropy
------------------------------------

-- repeadedly find a random partner, and exchange the infected state with them
-- if either of the partners are infected before, both are infected after
function anti_entropy_periodic()
  while true do
    events.sleep(gossip_interval)
    local exchange_partner_position = find_random_partner()
    debug(job.position .. " <--anti-entropy--> " .. exchange_partner_position)
    local exchange_partner = job.nodes[exchange_partner_position]
    local partner_is_infected = rpc.call(exchange_partner, {'anti_entropy_infect', infected, job.position})

    -- if the partner is infected, but I am not yet infected, then I have now become infected
    if partner_is_infected and not infected then
      debug('Position ' .. job.position .. ' got anti-entropy infected by ' .. exchange_partner_position)
      logS("i_am_infected_by_anti_entropy")
      infected = true
    end
  end
end


-- RPC call that gets called remotely by anti_entropy_periodic()
-- called to check if the node is infeced, and infects the node if the caller is infected
function anti_entropy_infect(caller_is_infected, senderPosition)
  -- if the person calling me is infected, and I am not yet infected, then I am now infected
  if caller_is_infected and not infected then
    logS("i_am_infected_by_anti_entropy")
    debug('Position ' .. job.position .. ' got anti-entropy infected by ' .. senderPosition)
    infected = true
  end
  return infected
end


-- select a random partner to exchange with
-- require that this partner is not oneself
function find_random_partner()
  local exchange_partner_position = 0
  repeat
    exchange_partner_position = math.random(#job.nodes)
  until exchange_partner_position ~= job.position
  return exchange_partner_position
end


------------------------------------
-- rumor mongering
------------------------------------

-- repeatedly check if there is a message buffered
-- if there is, select a number of random exchange partners
-- and send them the message
function rumor_mongering_periodic()
  while true do
    events.sleep(gossip_interval)
    if buffered then
      exchange_partner_positions = find_random_partners()
      for exchange_partner_position, _ in pairs(exchange_partner_positions) do
        exchange_partner = job.nodes[exchange_partner_position]
        rpc.call(exchange_partner, {'rumor_mongering_infect', buffered_hops_to_live, job.position})
        debug(job.position .. " <--rumor-mongering--> " .. exchange_partner_position)
      end
      buffered = false
    end
  end
end


-- RPC call that can be use to infect this node
-- if the node is not already infected, it becomes infected
-- if the node is not already buffering, it starts to buffer sending a message
-- if the node is already buffering, it takes the max hops_to_live from what it 
-- already has and what is gets from the current message
function rumor_mongering_infect(hops_to_live, senderPosition)
  if not infected then
    logS("i_am_infected_by_rumor_mongering")
    debug('Position ' .. job.position .. ' got infected by ' .. senderPosition)
    infected = true
  else
    debug('Position ' .. job.position .. ' received duplicate from ' .. senderPosition)
    logS("duplicate_received")
  end
  if not buffered or buffered and hops_to_live - 1 > buffered_hops_to_live then
    buffered_hops_to_live = hops_to_live - 1
    if buffered_hops_to_live > 0 then
      buffered = true
    end
  end
end
  

-- select a number random partners to exchange with
-- require that none of these partners is not oneself
-- and that every partner occurs only once
-- P.S. LUA is not a very expressive language... This would be one line in Python
function find_random_partners()
  local exchange_partner_positions = {}
  local found_partners = 0
  repeat
    repeat
      exchange_partner_position = math.random(#job.nodes)
    until (exchange_partner_position ~= job.position and (not exchange_partner_positions[exchange_partner_position]))
    exchange_partner_positions[exchange_partner_position] = true
    found_partners = found_partners + 1
  until found_partners == distribution_count
  return exchange_partner_positions
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

  -- start rumor mongering infinite loop
  if do_rumor_mongering then
    events.thread(rumor_mongering_periodic)
  end

  -- start anti entropy infinite loop
  if do_anti_entropy then
    events.thread(anti_entropy_periodic)
  end
  
  -- this thread will be in charge of killing the node after max_time seconds
  events.thread(terminator)
end  

events.thread(main)  
events.loop()

