/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.raft.storage.segstore;

/**
 * Mutable index memtable.
 *
 * <p>This class represents an in-memory index of the current segment file used by a {@link SegmentFileManager}. Index is
 * essentially a mapping from {@code [groupId, logIndex]} to the offset in the segment file where the corresponding log entry is stored.
 *
 * <p>It is expected that entries for each {@code groupId} are written by one thread, therefore concurrent writes to the same
 * {@code groupId} are not safe. However, reads from multiple threads are safe in relation to the aforementioned writes.
 */
interface MutableIndexMemTable {
    /**
     * Returns the offset in the segment file where the log entry with the given {@code logIndex} is stored or {@code 0} if the log entry
     * was not found in the memtable.
     */
    int getSegmentFileOffset(long groupId, long logIndex);

    /**
     * Appends a new segment file offset to the memtable.
     *
     * @param groupId Raft group ID.
     * @param logIndex Raft log index.
     * @param segmentFileOffset Offset in the segment file.
     */
    void appendSegmentFileOffset(long groupId, long logIndex, int segmentFileOffset);

    /**
     * Returns the immutable version of this memtable.
     */
    ImmutableIndexMemTable makeImmutable();
}
