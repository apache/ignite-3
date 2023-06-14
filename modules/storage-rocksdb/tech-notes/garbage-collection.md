# Garbage Collection in the RocksDB partition storage

This document describes the process of Garbage Collection in the RocksDB-based storage.
The goal of this process is to remove stale data based on a given timestamp.

## Garbage Collection algorithm

It's important to understand when we actually need to perform garbage collection. Older versions of rows can
be garbage collected if and only if there's a newer version of the row and this new version's timestamp
is below the low watermark. This low watermark indicates the minimal timestamp that a running transaction might have.

Consider the following example:  
*Note that **Record number** is a hypothetical value that helps referring to the specific entries, there
is no such value in the storage.*

| Record number | Row id | Timestamp |
|---------------|--------|-----------|
| 1             | Foo    | 1         |
| 2             | Foo    | 10        |

In this case, we can only remove record 1 if the low watermark is 10 or higher. If watermark is at 9,
then it means that there can still occur a transaction with a 9 timestamp, which means that the record number 1
is still needed.  
This is why we only add a new entry into the GC queue if there is a previous version and that is
why the timestamp of the entry in the GC queue is of the next version.

Let's review another example:  
*Note that **Is tombstone** is a hypothetical value that helps referring to the specific entries, there
is no such value in the storage.*

| Record number | Row id | Timestamp | Is tombstone |
|---------------|--------|-----------|--------------|
| 1             | Foo    | 1         | False        |
| 2             | Foo    | 10        | True         |
| 3             | Foo    | 20        | False        |

Everything said before stands for this example, however we can also remove the record number 2, because it is
a tombstone. So if the watermark is higher or equal to 10 and there is a transaction with timestamp higher than
10, then we either get an empty value if timestamp is less than 20, or we get a newer version.

So to sum up, the algorithm looks like this:

1. Get an element from the GC queue, exiting if the queue is empty
2. Add that element to the batch for removal from RocksDB, if the element's timestamp is below the watermark, exiting otherwise
3. Find an element in the data column family that corresponds to the element of GC queue. If a value doesn't exist, exit
4. Test if it is a tombstone, if yes, add it to the batch for removal
5. Seek for a previous version. If it doesn't exist, exit
6. Add that previous version to the batch for removal

You might notice that there are two cases when we can exit prematurely, apart from queue being empty.  
We might have not found a value that triggered the addition to the GC queue and/or the value that needs to be
garbage collected because GC can run in parallel. So if two parallel threads got the same element from the
queue, one of them might have already finished the GC and removed the elements.

## Garbage Collection queue

We store garbage collector's queue in the RocksDB column family in the following
format. The key:

| Partition id | Timestamp                                 | Row id         |
|--------------|-------------------------------------------|----------------|
| 2-byte       | 12-byte (8-byte physical, 4-byte logical) | 16-byte (uuid) |

The value is not stored, as we only need the key. We can make row id the value,
because for the ascending order processing of the queue we only need the timestamp,
however, multiple row ids can have same timestamp, so making row id a value requires storing a list of
row ids, hence the commit in this implementation of the storage becomes more sophisticated and, probably,
less performant.

Each time a row is being committed to the storage, we perform a check whether
there is already a value for this row. If there is one and both it and new version are not tombstones, we put
new commit's timestamp and row id into the GC queue. To understand why we only put new value's timestamp
please refer to the Garbage Collection [algorithm](#garbage-collection-algorithm).  
The queue is updated along with the data column family in a single batch and is destroyed when the storage
is being cleared or destroyed.

## Storage implications

To save space we don't store consecutive tombstones.
For example, if a user removes a certain row twice

```
storage.put(key, value); // Time 1
storage.delete(key); // Time 10
storage.delete(key); // Time 20
```

There should be one row with a value and one row with a tombstone, the tombstone being
the oldest one. This also simplifies the processing of the garbage collection queue.
So in the storage we will see something like:

| Key | Value       | Timestamp |
|-----|-------------|-----------|
| Foo | Bar         | 1         |
| Foo | <tombstone> | 10        |
