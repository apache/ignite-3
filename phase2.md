## Chain of assignments

https://issues.apache.org/jira/browse/IGNITE-23870
https://issues.apache.org/jira/browse/IGNITE-24089

For the purpose of HA track phase 2 we are introducing abstraction AssignmentsChain:
```
AssignmentsChain(List<AssignmentsLink> chain):
    AssignmentsLink lastLink(String nodeId) // returns last link where nodeId is presented
    
     AssignmentsLink replaceLast(Assignments newLast, long term, long index) // replace the last link in chaing with the new one
     
      AssignmentsLink addLast(Assignments newLast, long term, long index)

AssignmentsLink(Assignments assignments, long index, long term, AssignmentsLink next)
```

on every `stablePartAssignmentsKey` update we also update the `assignmentsChainKey` with the

```
currentAssignmentsChain = ms.get(assignmentsChainKey)

if (!pendingAssignments.force() && !pendingAssignments.fromReset()):
    AssignmentsChain.of(term, index, newStable)
elif (!pendingAssignments.force() && pendingAssignments.fromReset()):
    currentAssignmentsChain.replaceLast(newStable, term, index)
else:
    currentAssignmentsChain.addLast(newStable, term, index) 
```

## Start of the node
On the start of every node in the HA scope the main points, which must be analyzed:
- Should this node start in general
    - Simple case: one link in chain, must be processed as usual case https://issues.apache.org/jira/browse/IGNITE-23885
    - Case the pending or stable raft topology is alive (see the pseudo code above)
  >> QUESTION: are we sure that these 2 cases have no contradictions?
    - Case with the chain unwind to understand the current situation (see pseudocode above)
- Should we cleanup this node or not

### Pending or stable raft topology is alive
```
// method which collect and await state request results from list of nodes
Map<NodeId, Boolean> sendAliveRequest(List<String> nodes, request)

// true if majority of nodes alive
boolean majorityAlive(Map<NodeId, Boolean> responses)

cleanupAndFollowUsualRecoveryFlow()

onNodeStart:
    (stabe, pending) = (ms.get(stablePartAssignmentsKey), ms.get(pendingPartAssignmentsKey))
    request = new StateRequest()
    
    if (majorityAlive(sendAliveRequest(stable) || majorityAlive(sendAliveRequest(pending)):
       cleanupAndFollowUsualRecoveryFlow() 
       return
```

### Chain unwind
So, if the pending or stable raft topologies is not alive, we need to start the process of chain unwind. It means, that we need to find the link in the chain, which last (in the chronological meaning) has the user inputs.

```
    fullChain = ms.get(assignmentsChainKey)
    currentNodeLink = fullChain.lastLink(currentNode)
    currentNodeCfgIndex = currentNodeLink.index
    
    nodesToSend = currentLink.next.nodes() - currentLink.next.next.nodes()
    stateRequest = new StateRequest(currentNodeCfgIndex)
    responses = sendAliveRequest(nodesToSend, stateRequest)
    
```

#### Node response to the state request
```
onStateRequestReceived(cfgIndex):
    sendResponse(checkSelfAliveness() || hasUserInputs(cfgIndex)) 
    
// https://issues.apache.org/jira/browse/IGNITE-23880
checkSelfAliveness()

// https://issues.apache.org/jira/browse/IGNITE-23882
hasUserInputs(cfgIndex)
    startRaftOnFakeConfiguration()
    replayRaftLog()
    return lastUserCmdIdx > cfgIndex 
```
>> QUESTION: why does the checkSelfAliveness() is the action of the state request processing? under the target issue we describe the process of the whole raft group aliveness. but at the same time we start this process on all nodes from assignments by the state request handling.

### Handle state responses:
And as the last step we need to handle the state responses:
```
    if (majorityAlive(responses)):
        skip
    elif (hasUserInputs(cfgIndex)):
        startNode
    else:
        skip
```

>> QUESTION: it looks like in the algorithm we must unwind chain in the recursive manner, but I didn't find this step in any of our docs/videos, only the depth 1 unwind: currentLink.next - currentLink.next.next. Is it ok?

# Result algorithm
Short version of algo, see needed method descriptions above:
```
onNodeStart:
    (stabe, pending) = (ms.get(stablePartAssignmentsKey), ms.get(pendingPartAssignmentsKey))
    request = new StateRequest()
    
    if (majorityAlive(sendAliveRequest(stable) || majorityAlive(sendAliveRequest(pending)):
       cleanupAndFollowUsualRecoveryFlow() 
       return
       
    fullChain = ms.get(assignmentsChainKey)
    currentNodeLink = fullChain.lastLink(currentNode)
    currentNodeCfgIndex = currentNodeLink.index
    
    nodesToSend = currentLink.next.nodes() - currentLink.next.next.nodes()
    stateRequest = new StateRequest(currentNodeCfgIndex)
    responses = sendAliveRequest(nodesToSend, stateRequest 
    
    if (majorityAlive(responses)):
        skip
    elif (hasUserInputs(cfgIndex)):
        startNode
    else:
        skip

onStateRequestReceived(cfgIndex):
    sendResponse(checkSelfAliveness() || hasUserInputs(cfgIndex)) 
```
