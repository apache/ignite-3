# Client Direct Transaction Coordinator

The "lightweight client tx coordinator" is an optimization that allows clients to bypass the traditional coordinator-proxy pattern 
and communicate directly with partition owners.

This requires existing active client connections to the partition owner nodes.

## Two Operating Modes

1. Proxy Mode (Traditional): Client → Coordinator → Partition Owners
2. Direct Mapping Mode (Lightweight): Client → Partition Owners (directly)

## Protocol

1. Transaction Start (Piggybacked)
- Request: TX_ID_FIRST_DIRECT = -1L embedded in first data operation (e.g., TUPLE_UPSERT)
- Includes: Observable timestamp, read-only flag, timeout, options
- Response: Transaction ID, UUID, coordinator ID, timeout
- Location: ClientTableCommon#readTx

2. Direct Operations
- Request: TX_ID_DIRECT = 0L marker with enlistment metadata
- Includes: Enlistment token, transaction UUID, commit table ID, commit partition ID, coordinator UUID
- Sent to: Partition owner directly (not coordinator)
- Location: DirectTxUtils.writeTx

3. Transaction Commit
- Operation: ClientOp.TX_COMMIT = 44
- Includes: Resource ID + list of all direct enlistments (tableId, partitionId, consistentId, token)
- Server merges client enlistments with server-side enlistments
- Location: ClientTransactionCommitRequest.java

4. Transaction Discard (Cleanup)
- Operation: ClientOp.TX_DISCARD = 75
- When: Transaction fails or rollback needed
- Includes: Transaction UUID + list of (tableId, partitionId) pairs
- Purpose: Clean up direct enlistments on partition owners
- Location: ClientTransaction#sendDiscardRequests

## Lifecycle

1. Client creates lazy transaction (no network call)
2. First operation triggers:
    - DirectTxUtils.ensureStarted() checks if colocated
    - DirectTxUtils.writeTx() writes TX_ID_FIRST_DIRECT (-1)
    - Request sent directly to partition owner
    - Server starts transaction and returns metadata
    - DirectTxUtils.readTx() creates ClientTransaction

3. Subsequent operations:
    - DirectTxUtils.resolveChannel() picks partition owner
    - ClientTransaction.enlistDirect() tracks enlistment
    - DirectTxUtils.writeTx() writes TX_ID_DIRECT (0) + enlistment token
    - Direct communication with partition owner

4. Commit:
    - Collects all enlistments from ClientTransaction.enlisted map
    - Sends TX_COMMIT with enlistment metadata to coordinator
    - Coordinator merges client + server enlistments
    - Validates tokens and commits transaction