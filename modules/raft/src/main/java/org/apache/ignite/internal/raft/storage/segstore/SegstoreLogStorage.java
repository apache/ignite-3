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

import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.io.IOException;
import java.util.List;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryDecoder;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryEncoder;
import org.apache.ignite.raft.jraft.option.LogStorageOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite's {@link LogStorage} implementation.
 *
 * <p>Every storage instance is associated with a single Raft group, but multiple storage instances can share the same
 * {@link SegmentFileManager} instance meaning that they can share the same segment files.
 */
class SegstoreLogStorage implements LogStorage {
    private final long groupId;

    private final SegmentFileManager segmentFileManager;

    private volatile LogEntryEncoder logEntryEncoder;

    private volatile LogEntryDecoder logEntryDecoder;

    private volatile long firstLogIndexInclusive = -1;

    private volatile long lastLogIndexInclusive = -1;

    SegstoreLogStorage(long groupId, SegmentFileManager segmentFileManager) {
        if (groupId <= 0) {
            throw new IllegalArgumentException("groupId must be greater than 0: " + groupId);
        }

        this.groupId = groupId;
        this.segmentFileManager = segmentFileManager;
    }

    @Override
    public boolean init(LogStorageOptions opts) {
        logEntryEncoder = opts.getLogEntryCodecFactory().encoder();

        logEntryDecoder = opts.getLogEntryCodecFactory().decoder();

        firstLogIndexInclusive = segmentFileManager.firstLogIndexInclusiveOnRecovery(groupId);

        lastLogIndexInclusive = Math.max(-1, segmentFileManager.lastLogIndexExclusiveOnRecovery(groupId) - 1);

        return true;
    }

    @Override
    public boolean appendEntry(LogEntry entry) {
        appendEntryImpl(entry);

        long firstLogIndexInclusive = this.firstLogIndexInclusive;

        if (firstLogIndexInclusive == -1) {
            this.firstLogIndexInclusive = entry.getId().getIndex();
        }

        lastLogIndexInclusive = entry.getId().getIndex();

        return true;
    }

    private void appendEntryImpl(LogEntry entry) {
        try {
            segmentFileManager.appendEntry(groupId, entry, logEntryEncoder);
        } catch (IOException e) {
            throw new IgniteInternalException(INTERNAL_ERR, e);
        }
    }

    @Override
    public int appendEntries(List<LogEntry> entries) {
        for (LogEntry entry : entries) {
            appendEntryImpl(entry);
        }

        long firstLogIndexInclusive = this.firstLogIndexInclusive;

        if (firstLogIndexInclusive == -1) {
            this.firstLogIndexInclusive = entries.get(0).getId().getIndex();
        }

        lastLogIndexInclusive = entries.get(entries.size() - 1).getId().getIndex();

        return entries.size();
    }

    @Override
    public long getFirstLogIndex() {
        long firstLogIndexInclusive = this.firstLogIndexInclusive;

        // JRaft requires to return 1 as the first log index if there are no entries.
        return firstLogIndexInclusive == -1 ? 1 : firstLogIndexInclusive;
    }

    @Override
    public long getLastLogIndex() {
        // JRaft requires to return 0 as the last log index if there are no entries.
        return Math.max(0, lastLogIndexInclusive);
    }

    @Override
    public @Nullable LogEntry getEntry(long index) {
        try {
            return segmentFileManager.getEntry(groupId, index, logEntryDecoder);
        } catch (IOException e) {
            throw new IgniteInternalException(INTERNAL_ERR, e);
        }
    }

    @Override
    public long getTerm(long index) {
        LogEntry entry = getEntry(index);

        return entry == null ? 0 : entry.getId().getTerm();
    }

    @Override
    public boolean truncatePrefix(long firstIndexKept) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean truncateSuffix(long lastIndexKept) {
        try {
            segmentFileManager.truncateSuffix(groupId, lastIndexKept);
        } catch (IOException e) {
            throw new IgniteInternalException(INTERNAL_ERR, e);
        }

        lastLogIndexInclusive = lastIndexKept;

        return true;
    }

    @Override
    public boolean reset(long nextLogIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown() {
    }
}
