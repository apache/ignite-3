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

import org.jetbrains.annotations.Nullable;

/**
 * Mutable index memtable.
 *
 * <p>This class represents an in-memory index of the current segment file used by a {@link SegmentFileManager}. Index is
 * essentially a mapping from {@code [groupId, logIndex]} to the offset in the segment file where the corresponding log entry is stored.
 *
 * <p>It is expected that entries for each {@code groupId} are modified by one thread, therefore concurrent writes to the same
 * {@code groupId} are not safe. However, reads from multiple threads are safe in relation to the aforementioned writes.
 */
interface WriteModeIndexMemTable {
    /**
     * Returns information about a segment file for the given group ID or {@code null} if it is not present in this memtable.
     */
    @Nullable SegmentInfo segmentInfo(long groupId);

    /**
     * Appends a new segment file offset to the memtable.
     *
     * @param groupId Raft group ID.
     * @param logIndex Raft log index.
     * @param segmentFileOffset Offset in the segment file.
     */
    void appendSegmentFileOffset(long groupId, long logIndex, int segmentFileOffset);

    /**
     * Removes all offsets for the given Raft group which log indices are strictly larger than {@code lastLogIndexKept}.
     */
    void truncateSuffix(long groupId, long lastLogIndexKept);

    /**
     * Removes all offsets for the given Raft group which log indices are strictly smaller than {@code firstIndexKept}.
     */
    void truncatePrefix(long groupId, long firstIndexKept);

    /**
     * Returns the read-only version of this memtable.
     */
    ReadModeIndexMemTable transitionToReadMode();
}
