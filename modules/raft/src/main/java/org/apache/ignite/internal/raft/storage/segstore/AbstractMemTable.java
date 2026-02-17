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

import java.util.Map;

abstract class AbstractMemTable implements WriteModeIndexMemTable, ReadModeIndexMemTable {
    @Override
    public void appendSegmentFileOffset(long groupId, long logIndex, int segmentFileOffset) {
        // File offset can be less than 0 (it's treated as an unsigned integer) but never 0, because of the file header.
        assert segmentFileOffset != 0 : String.format("Segment file offset must not be 0 [groupId=%d]", groupId);

        Map<Long, SegmentInfo> memTable = memtable(groupId);

        SegmentInfo segmentInfo = memTable.get(groupId);

        if (segmentInfo == null) {
            segmentInfo = new SegmentInfo(logIndex);

            segmentInfo.addOffset(logIndex, segmentFileOffset);

            memTable.put(groupId, segmentInfo);
        } else if (segmentInfo.isPrefixTombstone()) {
            segmentInfo = new SegmentInfo(logIndex, segmentInfo.firstIndexKept());

            segmentInfo.addOffset(logIndex, segmentFileOffset);

            memTable.put(groupId, segmentInfo);
        } else {
            segmentInfo.addOffset(logIndex, segmentFileOffset);
        }
    }

    @Override
    public SegmentInfo segmentInfo(long groupId) {
        return memtable(groupId).get(groupId);
    }

    @Override
    public void truncateSuffix(long groupId, long lastLogIndexKept) {
        Map<Long, SegmentInfo> memtable = memtable(groupId);

        memtable.compute(groupId, (id, segmentInfo) -> {
            if (segmentInfo == null || lastLogIndexKept < segmentInfo.firstLogIndexInclusive()) {
                // If the current memtable does not have information for the given group or if we are truncating everything currently
                // present in the memtable, we need to write a special "empty" SegmentInfo into the memtable to override existing persisted
                // data during search.
                return new SegmentInfo(lastLogIndexKept + 1);
            } else if (segmentInfo.isPrefixTombstone()) {
                // This is a prefix tombstone inserted by "truncatePrefix".
                return new SegmentInfo(lastLogIndexKept + 1, segmentInfo.firstIndexKept());
            } else {
                return segmentInfo.truncateSuffix(lastLogIndexKept);
            }
        });
    }

    @Override
    public void truncatePrefix(long groupId, long firstIndexKept) {
        Map<Long, SegmentInfo> memtable = memtable(groupId);

        memtable.compute(groupId, (id, segmentInfo) -> {
            if (segmentInfo == null) {
                // The memtable does not have any information for the given group, we need to write a special "prefix tombstone".
                return SegmentInfo.prefixTombstone(firstIndexKept);
            } else {
                return segmentInfo.truncatePrefix(firstIndexKept);
            }
        });
    }

    @Override
    public void reset(long groupId, long nextLogIndex) {
        Map<Long, SegmentInfo> memtable = memtable(groupId);

        memtable.compute(groupId, (id, segmentInfo) -> {
            if (segmentInfo == null || segmentInfo.isPrefixTombstone() || nextLogIndex < segmentInfo.firstLogIndexInclusive()) {
                // The memtable does not have any information for the given group, we need to write a special "reset tombstone".
                return SegmentInfo.resetTombstone(nextLogIndex);
            } else {
                return segmentInfo.reset(nextLogIndex);
            }
        });
    }

    @Override
    public ReadModeIndexMemTable transitionToReadMode() {
        return this;
    }

    protected abstract Map<Long, SegmentInfo> memtable(long groupId);
}
