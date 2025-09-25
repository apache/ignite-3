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
import org.apache.ignite.raft.jraft.entity.codec.LogEntryEncoder;
import org.apache.ignite.raft.jraft.option.LogStorageOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;

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

        return true;
    }

    @Override
    public boolean appendEntry(LogEntry entry) {
        try {
            segmentFileManager.appendEntry(groupId, entry, logEntryEncoder);
        } catch (IOException e) {
            throw new IgniteInternalException(INTERNAL_ERR, e);
        }

        return true;
    }

    @Override
    public int appendEntries(List<LogEntry> entries) {
        for (LogEntry entry : entries) {
            appendEntry(entry);
        }

        return entries.size();
    }

    @Override
    public long getFirstLogIndex() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLastLogIndex() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LogEntry getEntry(long index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getTerm(long index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean truncatePrefix(long firstIndexKept) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean truncateSuffix(long lastIndexKept) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean reset(long nextLogIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown() {
    }
}
