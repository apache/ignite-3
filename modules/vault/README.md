# Apache Ignite Native Persistence (nextgen)
This document describes the architecture of Apache Ignite 3.x native persistence and highlights key differencies 
compared to the Apache Ignite 2.x native perisitence architecture.

## Replication log as recovery journal 
Apache Ignite 3.x uses log-based replication protocol (currently, Raft) to ensure data consistency on different nodes
in the cluster. This very same log is used as a logical WAL for local state machine recovery. This change allows for
several optimizations in the page memory persistence mechanics.

This document describes the architecture of the replication log as well as the optimized version of the native 
persistence.

## Replication log
The Raft log represents a key-value storage for which key is always a contiguously growing unbounded integer number.
In practice, it is sufficient to limit the key by a 64-bit unsigned integer. The key range consists of two segments:
committed entries (the larger part of the log) cannot be changed and are immutable, and not-yet-committed entries that 
can be truncated and be overwritten. The non-committed entries are rarely overwritten in practice. Additionally, the 
Raft log can be compacted: entries with key smaller than lower bound are thrown away as they were made durable in the
state machine and are no longer needed.

The replication log storage is based on WiscKey approach of log-structured storages. Multiple Raft group logs can be 
stored in a single storage, however, it is not neccessary to have a single storage for all Raft groups. The storage and
WAL of Raft log are combined and use immutable append-only segments containing log indices and state machine commands.
Segment header may contain a dictionary to reduce the size of Raft group IDs written in the segment. Each log entry 
contains the log index, Raft group ID and command payload (the payload itself also contains the log entry term which 
is essential to Raft protocol). Schematically, the segment structure can be represented as follows:

<pre>
+----------------+--------------------------------+--------------------------------+-----+
|                |             Entry 1            |             Entry 2            | ... |
+----------------+--------------------------------+--------------------------------+-----+
| Segment header | Log index | Group ID | Payload | Log index | Group ID | Payload | ... |
+----------------+--------------------------------+--------------------------------+-----+
</pre>

New entries are always written to the end of the segment, even if it is an overwrite entry with the log index which
was already written to the same Raft group. The Raft log maintains an index of log entries for each Raft log group
which is represented as a simple array of entry offsets from the beginning of the file. The array is complemented with
the base index of the first entry in the segment. The in-memory index is flushed on disk (checkpointed) only when all
entries in the segment are committed. Schematically, the index structure may be represented as follows:

<pre>
+----------------+----------------------------------------------+----------------------------------------------+-----+
|                |                Group 1 Index                 |                Group 2 Index                 | ... |
+----------------+----------------------------------------------+----------------------------------------------+-----+
|  Index header  | Len | Index Base | Offset 1 | Offset 2 | ... | Len | Index Base | Offset 1 | Offset 2 | ... | ... | 
+----------------+----------------------------------------------+----------------------------------------------+-----+
</pre>

Upon recovery, the Raft log component reads all log entries from non-checkpointed segments and repopulates the in-memory
index which will be later checkpointed.

## No-logging native persistence
Given that the Raft log is available as a separate component, we can now see how the page memory-based native 
perisitence uses the external log to avoid any WAL logging for recovery.

Similar to Ignite 2.x native persistence, all data structures are implemented over logical pages. This include data
pages, B+Tree, reuse and free lists, etc. Each Raft log command represents a single atomic change to the state machine.
When a command is applied, it changes some pages in the state machine data structures, marking them dirtly. Unlike 
Ignite 2.x, no physical or logical records are written to WAL when these changes are applied. As a result, after 
applying a set of changes, certain pages will be marked dirty and need to be checkpointed. To allow for effecient 
checkpointing, Ignite 3.x splits storage files into main, containing only contiguous range of pages, and auxiliary 
files, containing dirty pages from consequtive checkpoints. Pages in auxiliary files are not necessarily contiguous
and may have gaps. The files are ordered by the order in which they were written, forming a structure similar to an 
LSM-tree: when a page is read from disk, it is first looked up in the most recent auxiliary file, then in the second
auxiliary file, and so on, until the main storage file is reached. To allow for effecient search in auxiliary files, 
a small reverse index is stored with each file that maps page IDs to offsets in the file. The number of auxiliary 
files should be kept small enough so that the full inverse index is kept in-memory; however, if the merge process
does not keep up with the load, the indexes can be always evicted from memory and read on-demand. 

Schematically, the structure of the main and auxiliary checkpoint files may be represented as follows:

Main storage file:
<pre>
+--------+--------+--------+-----+
| Page 1 | Page 2 | Page 3 | ... |
+--------+--------+--------+-----+
</pre>

Auxiliary storage file:
<pre>
+-----------------------------------------------------------+-------------------------+
|                       Offsets Index                       |        Pages Data       |
+-----------+--------------------+--------------------+-----+---------+---------+-----+
| Index len | Page P1 ID, Offset | Page P2 ID, Offset | ... | Page P1 | Page P2 | ... | 
+-----------+--------------------+--------------------+-----+---------+---------+-----+
</pre>

### Recovery
If a process crashes when an auxiliary file is not completely written, the file is discarded upon recovery and Raft 
log commands are replayed again, forming another set of dirty pages that should be checkpointed.

As the number of auxiliary files grow, they can be merged back to the main storage file in the same order they were
originally written. When an auxiliary file is fully merged to the main storage file, it can be safely removed provided
that there are no threads intending to read from that file. If a process crashes in the middle of the merge, the file
can be safely merged again (the original pages will not be used anyway).