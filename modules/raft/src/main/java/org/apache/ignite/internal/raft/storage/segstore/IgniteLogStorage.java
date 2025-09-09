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

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.raft.storage.segstore.SegmentFile.WriteBuffer;
import org.apache.ignite.internal.util.FastCrc;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryEncoder;
import org.apache.ignite.raft.jraft.option.LogStorageOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;

/**
 * Ignite's {@link LogStorage} implementation.
 *
 * <p>Every storage instance is associated with a single Raft group, but multiple storage instances can share the same
 * {@link SegmentFileManager} instance meaning that they can share the same segment files.
 *
 * <p>Every appended entry is converted into its serialized form (a.k.a. "payload"), defined by a {@link LogEntryEncoder},
 * and stored in a segment file.
 *
 * <p>Binary representation of each entry is as follows:
 * <pre>
 * +---------------+---------+--------------------------+---------+----------------+
 * | Raft Group ID (8 bytes) | Payload Length (4 bytes) | Payload | Hash (4 bytes) |
 * +---------------+---------+--------------------------+---------+----------------+
 * </pre>
 */
class IgniteLogStorage implements LogStorage {
    static final int GROUP_ID_SIZE_BYTES = Long.BYTES;

    static final int LENGTH_SIZE_BYTES = Integer.BYTES;

    static final int HASH_SIZE = Integer.BYTES;

    private final long groupId;

    private final SegmentFileManager segmentFileManager;

    private volatile LogEntryEncoder logEntryEncoder;

    IgniteLogStorage(long groupId, SegmentFileManager segmentFileManager) {
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
        byte[] bytes = logEntryEncoder.encode(entry);

        try (WriteBuffer writeBuffer = segmentFileManager.reserve(entrySize(bytes))) {
            writeEntry(writeBuffer, bytes);
        } catch (Exception e) {
            throw new IgniteInternalException(INTERNAL_ERR, e);
        }

        return true;
    }

    private void writeEntry(WriteBuffer writeBuffer, byte[] payload) {
        ByteBuffer buffer = writeBuffer.buffer();

        int pos = buffer.position();

        buffer
                .putLong(groupId)
                .putInt(payload.length)
                .put(payload);

        int dataSize = buffer.position() - pos;

        // Rewind the position for CRC calculation.
        buffer.position(pos);

        int crc = FastCrc.calcCrc(buffer, dataSize);

        // After CRC calculation, the position will be at the provided end of the buffer.
        buffer.putInt(crc);
    }

    static int entrySize(byte[] payload) {
        return GROUP_ID_SIZE_BYTES + LENGTH_SIZE_BYTES + payload.length + HASH_SIZE;
    }

    @Override
    public int appendEntries(List<LogEntry> entries) {
        entries.forEach(this::appendEntry);

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
