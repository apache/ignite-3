# ro:
- has read ts
- txn that created WI may be pending and remain pending after WI resolution

# rw:
Final tx state needed, because there is a need of WI resolution because:
- WI is not under lock
- primary changed and we cannot commit this transaction (and there was no graceful primary switch because WI is not under lock)

So, if rw txn performs WI resolution then the txn is either already finished or should be aborted (fixed in https://issues.apache.org/jira/browse/IGNITE-27255 )

# Before commit partition path:
we do local check first, then try to resolve state from coordinator, like it happens now.

# On commit partition:
- if commit partition has volatile non-final state, and there is no coordinator, commit partition aborts the txn.
- if commit partition doesnt have neither volatile nor persistent state this means one of two (both mean primary resolution path):
  - txn is not finished, volatile state is lost
  - txn was finished, state was vacuumized

Assuming that the resolution request comes to commit partition from node N to commit partition leaseholder C,
before doing request on primary, C gets current primary P and compares its consistency token with the consistency token from request from node N.
- if request consistency token is null, the request is not from primary replica, proceed,
- if request consistency token is the same as current primary consistency token (probably the primary was moved to N after it did the request or whatever)
  - if request has read timestamp (initiated by RO txn) - we can proceed with doing request to N (consider it is rare enough to do micro optimizations)
  - if request was initiated by RW txn - this means N is current primary, it has the most recent state of the row, and there is WI so it was not cleaned up on group majority - this means the txn was never finished and can be aborted
- if request consistency token is NOT equal to current primary consistency token:
  - if request has read timestamp (initiated by RO txn) - proceed
  - if request was initiated by RW txn - respond with error, N must abort its operation and respond with error to client (primary changed, the current transaction will not be able to be committed)

# cases after C sends request to P:
### case 0 (WI is committed on primary):
- tx2 reads WI on node N created by tx0, node N is backup of group G with primary replica P
- state of tx0 (including persistent state) is absent everywhere
- node N tries to resolve the state of tx0 from commit partition leaseholder C
- C doesn't have state of tx0 and makes request to P- on P, the state of WI is committed
- B doesn't write persistent state (because it was written before), cashes volatile state, responds with COMMITTED to N

### case 1 (WI is NOT finished on primary, 4 first steps are same as case 0):
- tx2 reads WI on node N created by tx0, node N is backup of group G with primary replica P
- state of tx0 (including persistent state) is absent everywhere
- node N tries to resolve the state of tx0 from commit partition leaseholder C
- C doesn't have state of tx0 and makes request to P
- on P, the state of WI is NOT finished, this means the volatile state was lost on commit partition, and persistent state was never written, because it can be vacuumized only after WI is finished on majority (and primary) of G
- C finishes tx0, responds to N with ABORTED state (regular recovery)

### case 2 (and more)
- tx2 reads WI0 on node N created by tx0, node N is backup of group G with primary replica P
- state of tx0 (including persistent state) is absent everywhere
- node N tries to resolve the state of tx0 from commit partition leaseholder C
- C doesn't have state of tx0 and makes request to P (with read timestamp, if tx2 is RO)
- on P, storage may have completely different state. Further actions:
  - if request has read timestamp because tx2 is RO:
  - P selects version corresponding this timestamp,
  - if later record is present and matches the read timestamp, then responds with this later record,
  - if WI0 is committed and matches the read timestamp, then responds to C with COMMITTED state,
  - if absent (aborted) but earlier record is present and matches the read timestamp, then responds with ABORTED state,
  - if absent (aborted) but another WI1 of transaction tx1 is written, then P resolves WI1 using read timestamp from the request, if WI1 can be read then responds with newest record, if WI1 is ignored then responds with ABORTED (because WI0 is aborted)
  - if absent (aborted) and no records correspond timestamp, then checks read ts:
  - if read ts is greater than now - dataAvailabilityTimeout, responds with ABORTED state,
  - if read ts is less than or equal to now - dataAvailabilityTimeout, respond with error (outdated ro txn), this means the version was collected by GC and state is unknown
  - the case when request does not have read timestamp (meaning that tx2 is RW txn) is not possible.
- C gets from P either tx state or more recent row, responds to N (resolution response should be extended with row, its commit timestamp, etc)

## Also
- check that write intents are switched on primary replica (i.e. they must be switched on primary before cleanup is considered as completed) 