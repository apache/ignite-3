## Chain of assignments

https://issues.apache.org/jira/browse/IGNITE-23870

https://issues.apache.org/jira/browse/IGNITE-24089

For the purpose of HA track phase 2 we are introducing abstraction AssignmentsChain:
```
AssignmentsChain(List<AssignmentsLink> chain):
    AssignmentsLink lastLink(String nodeId) // returns last link where nodeId is presented
    
    AssignmentsLink replaceLast(Assignments newLast, long term, long index) // replace the last link in chain with the new one
     
    AssignmentsLink addLast(Assignments newLast, long term, long index)

AssignmentsLink(Assignments assignments, long index, long term, AssignmentsLink next)
```

on every `stablePartAssignmentsKey` update we also update the `assignmentsChainKey` with the

```
currentAssignmentsChain = ms.getLocally(assignmentsChainKey)

if (!pendingAssignments.force() && !pendingAssignments.fromReset()):
    newChain = AssignmentsChain.of(term, index, newStable)
elif (!pendingAssignments.force() && pendingAssignments.fromReset()):
    newChain = currentAssignmentsChain.replaceLast(newStable, term, index)
else:
    newChain = currentAssignmentsChain.addLast(newStable, term, index) 

ms.put(assignmentsChainKey, newChain) 
```

## Start of the node
On the start of every node in the HA scope the main points, which must be analyzed:
- Should this node start in general
    - Simple case: one link in chain, must be processed as usual case https://issues.apache.org/jira/browse/IGNITE-23885
    - Case the pending or stable raft topology is alive (see the pseudo code above)
    - Case with the chain unwind to understand the current situation (see pseudocode above)
- Should we cleanup this node or not

### Pending or stable raft topology is alive
```
// method which collect and await alive request results from list of nodes
Map<NodeId, Boolean> sendAliveRequests(List<String> nodes)

// true if majority of nodes alive
boolean majorityAlive(Map<NodeId, Boolean> responses)

cleanupAndFollowUsualRecoveryFlow()

// TODO: this check must be defined in future
// https://issues.apache.org/jira/browse/IGNITE-23880
checkSelfAliveness()

onAliveRequestReceive:
    sendResponse(checkSelfAliveness())

onNodeStart:
    (stabe, pending) = (ms.getLocally(stablePartAssignmentsKey), ms.getLocally(pendingPartAssignmentsKey))
    request = new StateRequest()
    
    if (majorityAlive(sendAliveRequests(stable) || majorityAlive(sendAliveRequests(pending)):
       cleanupAndFollowUsualRecoveryFlow() 
       return
```

> Node aliveness is a separate non-trivial topic to discuss. High level vision: node is alive if it has an appropriate peer configuration
> and functioning well.
> We need to formalise this definition better.

### Chain unwind
So, if the pending or stable raft topologies is not alive, we need to start the process of chain unwind.
It means, that we need to find the link in the chain, which last (in the chronological meaning) has the user inputs.

```
bool checkIfChainHasUserInputs():
    fullChain = ms.getLocally(assignmentsChainKey)
    currentNodeLink = fullChain.lastLink(currentNode)
    currentNodeCfgIndex = currentNodeLink.index
    
    nodesToSend = currentLink.next.nodes() - currentLink.next.next.nodes()
    hasUserInputsRequest = new HasUserInputsRequest(currentNodeCfgIndex)
    return anyMatchTrue(sendHasUserInputsRequest(nodesToSend, hasUserInputsRequest))

// request for the user inputs after the last RAFT group reconfiguration 
HasUserInputsRequest(cfgIndex) 

Map<NodeId, Boolean> sendHasUserInputsRequest(List<String> nodes, HasUserInputsRequest request)
```

#### Node response to the HasUserInputsRequest
```
onHasUserInputsRequestReceived(request):
    if (hasUserInputs(cfgIndex)):
        sendResponse(true)
    else:
        sendResponse(checkIfChainHasUserInputs())
    
// https://issues.apache.org/jira/browse/IGNITE-23882
hasUserInputs(cfgIndex)
    startRaftOnFakeConfiguration()
    replayRaftLog()
    return lastUserCmdIdx > cfgIndex 
```

### Handle state responses:
And as the last step we need to handle the state responses:
```
    if (checkIfChainHasUserInputs()):
        eraseLocalData()
    elif (hasUserInputs(cfgIndex)):
        startNode
    else:
        eraseLocalData()
```

## Result algorithm
Short version of algo, see needed method descriptions above:
```
onNodeStart:
    (stabe, pending) = (ms.getLocally(stablePartAssignmentsKey), ms.getLocally(pendingPartAssignmentsKey))
    request = new StateRequest()
    
    if (majorityAlive(sendStateRequest(stable) || majorityAlive(sendStateRequest(pending)):
       cleanupAndFollowUsualRecoveryFlow() 
       return
       
    if (checkIfChainHasUserInputs()):
        eraseLocalData()
    elif (hasUserInputs(cfgIndex)):
        startNode
    else:
        eraseLocalData()
        
onAliveRequestReceive:
    sendResponse(checkSelfAliveness())
    
onHasUserInputsRequestReceived(request):
    if (hasUserInputs(cfgIndex)):
        sendResponse(true)
    else:
        sendResponse(checkIfChainHasUserInputs())
```

## Optimisations
- In general, we can piggyback hasUserInputs flag on the first aliveness checks and use one request to check the aliveness
and to receive the user inputs.
- We can remove redundant requests, if will prepare the full list of target nodes in pending/stable/chain
and send only on request for each node.
- If we find the one node with user inputs - we can stop without waiting for another responses.
