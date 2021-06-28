# Ignite transactions
This module provides transactions support for cross partition operations. Using the transactions, such operations are
executed in atomic way (either all changes all applied, or nothing at all) with a serializable isolation.

Transactions support is supposed to be icremental. In the first approach, we are trying to put existing ideas from
ignite 2 to the new replication infrastructure. In the next phases, MVCC support shoudl be added to avoid blocking reads 
and some other optimization, like parallel commits from <sup id="a1">[1](#f1)</sup>

# Transaction protocol design

In high level, we utilize 2 phase locking (2PL) for a concurrency control, 2 phase commit (2PC) as an atomic commitment 
protocol, in conjunction with WAIT_DIE deadlock prevention, described in <sup id="a2">[2](#f2)</sup>. 
This implementation is very close to Ignite 2 optimistic serializable mode. 
Additional goals are: 
1) retain only Serializable isolation 
2) support for SQL 
3) utilize new common replication infrastructure based on RAFT.

# Two phase commit
This protocol is responsible for atomic commentment (all or nothing) tx guraranties.
Each update is **pre-written** to a replication groups on first phase (and replicated to majority).
As soon as all updates are pre-written, it's safe to commit.
Slightly differs from ignite 2, has not PREPARED state.


# Two phase locking
A transaction consists of read and write operations. 2PL states the transaction constist of growing phase,
where locks are acquired, and shrinking phase where locks are released.

A tx holds shared locks for reads and exclusive locks for writes.
It's possible to lock for read in exclusive mode (select for update semantics)

Locks for different keys can be acquired in parallel.

Shared lock is obtained on DN-read operation.
Exclusive lock is obtained on DN-prewrite operation.

# Lock manager
Locking functionality is implemented by LockManager. 
Each **leaseholder** for a partition replication group deploys an instance of LockManager. 
All reads and writes go through the **leaseholder**. Only raft leader for some term can become a **leaseholder**.
It's important what no two leaseholders existance intervals can overlap for the same group. 
Current leasholder map can be loaded from metastore (watches can be used for a fast notification about leaseholder change).

# Locking precausion
LockManager has a volatile state, so some precausions must be taken before locking the keys due to possible node restarts.
Before taking a lock, LockManager should consult a tx state for the key. If a key is enlisted in transaction,
lock can't be taken immediately.

# Tx coordinator
Tx coordinator is assigned in random fashion from a list of allowed nodes. They can be same as data nodes.
They are responsible for id assignment and failover handling if some nodes from tx topology have failed. 
Knows full tx topology.

# Deadlock prevention
Deadlock prevention in WAIT_DIE mode - uses priorities to decide which tx should be restarted.
Each transaction is assigned a unique globally comparable timestamp (for example UUID), which defines tx priority.
If T1 has lower priority when T2 (T1 is younger) it can wait for lock, otherwise it's restarted keeping it's timestamp.
committing transaction can't be restarted.
Deadlock detection is not an option due to huge computation resources requirement and no real-time guaranties.

# Tx map
Each node maintains a persistent tx map:
txid -> txstate(PENDING|ABORTED|COMMITED)
This map is used for tx failover.

# Failover handling

## Leaserholder fail

## Coordinator fail

<em id="f1">[1]</em> CockroachDB: The Resilient Geo-Distributed SQL Database. [↩](#a1)<br/>
<em id="f2">[2]</em> Concurrency Control in Distributed Database Systems. [↩](#a2)
 