-- SPLAYschool tutorial
-- BASE libraries (threads, events, sockets, ...)

require"splay.base"

-- RPC library

rpc = require"splay.rpc"

-- log
local log = require"splay.log"
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

-- create thread to execute the main function
events.thread(SPLAYschool)

-- start the application
events.loop()

-- now, you can watch the logs of your job and enjoy ;-)
-- try this job with multiple splayds and different parameters

