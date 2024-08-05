# Catching up process

## Entry point:

We have a closure named `NodeImpl.OnCaughtUp`, which is responsible for the catching up process for every stale node/replicator on a leader.
This closure is created every time we call `NodeImpl.ConfigurationCtx#addNewPeers` which happens on a raft configuration change, for example
when we call `NodeImpl#changePeerAndLearners`. In `NodeImpl.ConfigurationCtx#addNewPeers` method we assign `OnCaughtUp` closure with a corresponding
replicator for a stale node. This is done inside `ReplicatorGroupImpl#waitCaughtUp`
by calling `Replicator#waitForCaughtUp`. To be more precise, we save the closure in a field `Replicator#catchUpClosure` and also we schedule
timer on a replicator to call `Replicator#onCatchUpTimedOut` (by default it is called after election timeout).

## Closure invocation

So, we saved closure, it is time to discuss what happens when the closure is run. It happens in `Replicator#notifyOnCaughtUp`, we call this
method with a status of success or failure of a catching up process and propagate it to the closure by setting `Status.setError(int,
java.lang.String, java.lang.Object...)`. When the closure is run, `NodeImpl#onCaughtUp` is called, this method checks the status of a process
and here we have several outcomes:

- `Status` is `OK` and `NodeImpl.ConfigurationCtx#onCaughtUp` with a success flag equals true is called, so we can move
  to `NodeImpl.ConfigurationCtx.nextStage` in a configuration changing process.
- `Status` is `Error` and more specific, it is timeout, so we retry catching up process by creating a new `NodeImpl.OnCaughtUp` closure
  calling the same `Replicator#waitForCaughtUp` that we described before.
- If retrying went wrong or `Status` is `Error` and is not a timeout, we call `NodeImpl.ConfigurationCtx#onCaughtUp` with a success flag
  equals false, so the whole process of a configuration changing is reset with `RaftError.ECATCHUP` and corresponding
  `RaftGroupEventsListener#onReconfigurationError` is called. Note that we do not preserve the original reason for failed catch up. Also, it
  is important, that `RaftGroupEventsListener#onReconfigurationError` can be called, when a current leader stepped down. In that case
  `ConfigurationCtx#reset(org.apache.ignite.raft.jraft.Status)` with status equals `null` will be called, and this `null` value will be
  passed to `RaftGroupEventsListener#onReconfigurationError`.

## Where the closure is invoked 

Now let's discuss when this closure is run. As we said before, it happens in `Replicator#notifyOnCaughtUp`, 
so lets track who call `Replicator#Replicator#notifyOnCaughtUp`

Calls with successful statuses: 
- `Replicator#onInstallSnapshotReturned` -- called after a successful installation of a snapshot
- `Replicator#onAppendEntriesReturned` -- called after a successful appending of new entries from a leader

Calls with error statuses:

* `Replicator#onCatchUpTimedOut` with `RaftError.ETIMEDOUT`, called when a timer event happens. As was described before, this timer is started
  when we call `Replicator#waitForCaughtUp`.
* `Replicator#onHeartbeatReturned`, `Replicator#onAppendEntriesReturned`, or `Replicator#onTimeoutNowReturned` with `RaftError.EPERM`,
  called when a follower returns a term higher than a leader's current term. This is a general check for RPC calls where we check terms and
  decide, should we step down or not.
* `Replicator#onTimeoutNowReturned` with `RaftError.ESTOP`, called when we passed to the method flag `stopAfterFinish` equals true. It happens
  when a leader is stepped down and we try to wake up a potential candidate for the optimisation purposes 
(see [waking up optimisation](#Waking-up-optimisation)), so we call
  `Replicator#sendTimeoutNowAndStop(this.wakingCandidate, this.options.getElectionTimeoutMs())` on a leader. For more details see
  `NodeImpl#stepDown`
* `Replicator#onError` with `RaftError.ESTOP`. This is a general case when some replicator was stopped. For example, it might happen when
  a leader stepped down, or when a node was shutdown, etc. Let's consider all places where `Replicator#onError` with `RaftError.ESTOP` can happen, to
  do that we need to trace `Replicator#stop`
  - `NodeImpl#shutdown(org.apache.ignite.raft.jraft.Closure)` -- node shutdown case 
  - `ReplicatorGroupImpl#stopAll` -- happens when a leader steps down, including stopping all replicators. See `NodeImpl#stepDown`. 
  - `ReplicatorGroupImpl#stopReplicator` -- this happens when we call `ConfigurationCtx#reset(org.apache.ignite.raft.jraft.Status)`, 
  when we successfully or not successfully changed configuration, so we have to start or stop replicators for new peers. 
  - `ReplicatorGroupImpl#stopAllAndFindTheNextCandidate` -- called when a leader step down, in case we make
    [waking up optimisation](#Waking-up-optimisation)

  
## Waking up optimisation

When a leader faces some problem, it makes some optimisation when it steps down, to start a new voting with a new candidate immediately. In
that case, instead of stopping all replicators as usual, it preserves one replicator for stopping and sends `TimeoutNowRequest` to it. When
the node receives that request, it elects itself and starts voting. Failed leader chose such a node by searching for the node with the largest 
log id among peers in the current configuration. For more details see `ReplicatorGroupImpl#stopAllAndFindTheNextCandidate` 
