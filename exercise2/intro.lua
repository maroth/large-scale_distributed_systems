require("splay.base")
rpc = require("splay.rpc")

if not job then
  -- can NOT be required in SPLAY deployments !
  local utils = require("splay.utils")
  if #arg < 2 then
    print("lua "..arg[0].." my_position nb_nodes")
    os.exit()
  else
    local pos, total = tonumber(arg[1]), tonumber(arg[2])
    job = utils.generate_job(pos, total, 20001)
  end
end

-- log
local log = require("splay.log")
local l_o = log.new(3, "[school]")

-- accept incoming RPCs

rpc.server(job.me.port)
function call_me(position)
    log:print("I received an RPC from node "..position)
end

-- our main function
function SPLAYschool()
    local nodes = job.nodes()
    -- print bootstrap information about local node
    l_o:print("I’m "..job.me.ip..":"..job.me.port)
    l_o:print("My position in the list is: "..job.position)
    l_o:print("List type is ’"..job.list_type.."’ with "..#nodes.." nodes")
    -- wait for all nodes to be started (conservative)
    events.sleep(5)
    -- send RPC to random node of the list
    rpc.call(nodes[1], {"call_me", job.position})
    -- you can also spawn new threads (here with an anonymous function)
    events.thread(function() log:print("Bye bye") end)
    -- wait for messages from other nodes
    events.sleep(5)
    -- explicitly exit the program (necessary to kill RPC server)
    os.exit()
end

events.run(SPLAYSchool)

-- create thread to execute the main function
-- events.thread(SPLAYschool)

-- start the application
-- events.loop()

-- now, you can watch the logs of your job and enjoy ;-)
-- try this job with multiple splayds and different parameters

