------------------------------------
-- ANTI-ENTROPY
------------------------------------
-- The Anti-Entropy dissemination method
-- By Markus Roth


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
      local strT = ts.year..'-'..ts.month..'-'..ts.day..' '..ts.hour..':'..ts.min..':'..ts.sec..' ('..job.position..') '..info
      print(strT)
    end
  end
else
  logS = print
end

rpc.server(job.me.port)

------------------------------------
-- constants
------------------------------------

-- gossip interval in seconds
anti_entropy_period = 5  

-- total running time
max_time = 120 

------------------------------------
-- variables
------------------------------------
infected = "no"


------------------------------------
-- helper functions
------------------------------------

-- terminate the program after a given amount of time
function terminator()
  events.sleep(max_time)
  --logS("FINAL: node "..job.position.." "..infected) 
  os.exit()
end

-- select a random partner to exchange with
-- require that this partner is not oneself
function find_random_partner()
  repeat
    exchange_partner_position = math.random(#job.nodes)
  until exchange_partner_position ~= job.position
  return exchange_partner_position
end

-- repeadedly find a random partner, and exchange the infected state with them
-- if either of the partners are infected before, both are infected after
function anti_entropy()
  while true do
    events.sleep(anti_entropy_period)
    exchange_partner_position = find_random_partner()
    -- logS(job.position .. " <--> " .. exchange_partner_position)
    exchange_partner = job.nodes[exchange_partner_position]
    partner_is_infected = rpc.call(exchange_partner, {'infect', infected, job.position})

    -- if the partner is infect, but I am not yet infected, then I have now become infected
    if partner_is_infected == "yes" and infected == "no" then
      -- logS('Position ' .. job.position .. ' got infected by ' .. exchange_partner_position)
      logS("i_am_infected")
      infected = "yes"
    end
  end
end

-- RPC call that is the opposite part of anti_entropy()
-- called to check if the node is infeced, and infects the node if the caller is infected
function infect(value, senderPosition)
  -- if the person calling me is infected, and I am not yet infected, then I am now infected
  if value == "yes" and infected == "no" then
    logS("i_am_infected")
    -- logS('Position ' .. job.position .. ' got infected by ' .. senderPosition)
    infected = "yes"
  end
  return infected
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
  local desync_wait = (anti_entropy_period * math.random())

  -- the first node is the source and is infected since the beginning
  if job.position == 1 then
    infected = "yes"
    --logS(job.position .. ' got infected as patient zero')
    logS("i_am_infected")
    desync_wait = 0
  end
  --logS("waiting for "..desync_wait.." to desynchronize")
  events.sleep(desync_wait)  

  --start the gossip loop
  events.thread(anti_entropy)
  
  -- this thread will be in charge of killing the node after max_time seconds
  events.thread(terminator)
end  

events.thread(main)  
events.loop()

