#!/usr/bin/lua
require"splay.base"
rpc = require"splay.urpc"
-- to use TCP RPC, replace previous by the following line
-- rpc = require"splay.rpc"

-- addition to allow local run
if not job then
  -- can NOT be required in SPLAY deployments !  
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

logS('Hello!!!')
rpc.server(job.me.port)

-- constants
anti_entropy_period = 5 -- gossip every 20 seconds
max_time = 120 -- we do not want to run forever ...

-- variables
infected = "no"
current_cycle = 0

-- TODO here insert your functions from the gossip framework

--
--
--
--
--

-- helping functions
function terminator()
  events.sleep(max_time)
  logS("FINAL: node "..job.position.." "..infected) 
  os.exit()
end

function test(senderP)
  logS('My position is: '..job.position)
  logS('And I receive a callback from node located at: '..senderP)
end

function main()
  events.sleep(2)
  if job.position == 1 then
    logS('Calling node at position 2...')
    rpc.call(job.nodes[2], {'test', job.position})
    logS('DONE')
    logS('Calling node at position 3...')
    rpc.call(job.nodes[3], {'test', job.position})
    logS('DONE')
  end
  logS('Waiting...')
  events.sleep(6)
  logS('THE END')
  os.exit()
  -- init random number generator
  math.randomseed(job.position*os.time())
  -- wait for all nodes to start up (conservative)
  events.sleep(2)
  -- desynchronize the nodes
  local desync_wait = (anti_entropy_period * math.random())
  -- the first node is the source and is infected since the beginning
  if job.position == 1 then
    infected = "yes"
    logS(job.position.." i_am_infected")
    desync_wait = 0
  end
  logS("waiting for "..desync_wait.." to desynchronize")
  events.sleep(desync_wait)  
  
  -- TODO: here, you should insert the command 
  --       that starts the gossiping activity
  
  -- this thread will be in charge of killing the node after max_time seconds
  events.thread(terminator)
end  

events.thread(main)  
events.loop()

