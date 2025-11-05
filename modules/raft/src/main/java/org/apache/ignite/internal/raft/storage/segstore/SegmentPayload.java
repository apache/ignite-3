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

import java.nio.ByteBuffer;
import org.apache.ignite.internal.raft.util.VarlenEncoder;
import org.apache.ignite.internal.util.FastCrc;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryDecoder;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryEncoder;

/**
 * Describes a payload entry in a segment file.
 *
 * @see SegmentFileManager
 */
class SegmentPayload {
    static final int GROUP_ID_SIZE_BYTES = Long.BYTES;

    static final int LENGTH_SIZE_BYTES = Integer.BYTES;

    static final int HASH_SIZE_BYTES = Integer.BYTES;

    /**
     * Length of the byte sequence that is written when suffix truncation happens.
     *
     * <p>Format: {@code groupId, TRUNCATE_SUFFIX_RECORD_MARKER (special length value), last kept index, crc}
     */
    static final int TRUNCATE_SUFFIX_RECORD_SIZE = GROUP_ID_SIZE_BYTES + LENGTH_SIZE_BYTES + Long.BYTES + HASH_SIZE_BYTES;

    /**
     * Length of the byte sequence that is written when prefix truncation happens.
     *
     * <p>Format: {@code groupId, TRUNCATE_PREFIX_RECORD_MARKER (special length value), first kept index, crc}
     */
    static final int TRUNCATE_PREFIX_RECORD_SIZE = TRUNCATE_SUFFIX_RECORD_SIZE;

    static final int TRUNCATE_SUFFIX_RECORD_MARKER = 0;

    static final int TRUNCATE_PREFIX_RECORD_MARKER = -1;

    static void writeTo(
            ByteBuffer buffer,
            long groupId,
            int segmentEntrySize,
            LogEntry logEntry,
            LogEntryEncoder logEntryEncoder
    ) {
        int originalPos = buffer.position();

        buffer
                .putLong(groupId)
                .putInt(segmentEntrySize - fixedOverheadSize());

        LogId logId = logEntry.getId();

        VarlenEncoder.writeLong(logId.getIndex(), buffer);
        VarlenEncoder.writeLong(logId.getTerm(), buffer);

        logEntryEncoder.encode(buffer, logEntry);

        int recordSize = buffer.position() - originalPos;

        writeCrc(buffer, recordSize);
    }

    static void writeTruncateSuffixRecordTo(ByteBuffer buffer, long groupId, long lastLogIndexKept) {
        buffer
                .putLong(groupId)
                .putInt(TRUNCATE_SUFFIX_RECORD_MARKER)
                .putLong(lastLogIndexKept);

        writeCrc(buffer, TRUNCATE_SUFFIX_RECORD_SIZE - HASH_SIZE_BYTES);
    }

    static void writeTruncatePrefixRecordTo(ByteBuffer buffer, long groupId, long firstIndexKept) {
        buffer
                .putLong(groupId)
                .putInt(TRUNCATE_PREFIX_RECORD_MARKER)
                .putLong(firstIndexKept);

        writeCrc(buffer, TRUNCATE_PREFIX_RECORD_SIZE - HASH_SIZE_BYTES);
    }

    private static void writeCrc(ByteBuffer buffer, int recordSizeWithoutCrc) {
        buffer.position(buffer.position() - recordSizeWithoutCrc);

        int crc = FastCrc.calcCrc(buffer, recordSizeWithoutCrc);

        buffer.putInt(crc);
    }

    static LogEntry readFrom(ByteBuffer buffer, LogEntryDecoder logEntryDecoder) {
        int originalPosition = buffer.position();

        buffer.position(originalPosition + GROUP_ID_SIZE_BYTES); // Skip group ID.

        int payloadLength = buffer.getInt();

        int payloadPosition = buffer.position();

        VarlenEncoder.readLong(buffer); // Skip log entry index.
        VarlenEncoder.readLong(buffer); // Skip log entry term.

        int logEntryPosition = buffer.position();

        int crcPosition = payloadPosition + payloadLength;

        int crc = buffer.getInt(crcPosition);

        buffer.position(originalPosition);

        int actualCrc = FastCrc.calcCrc(buffer, crcPosition - originalPosition);

        if (crc != actualCrc) {
            throw new IllegalStateException("CRC mismatch, expected: " + crc + ", actual: " + actualCrc);
        }

        buffer.position(logEntryPosition);

        // TODO: https://issues.apache.org/jira/browse/IGNITE-26623.
        byte[] entryBytes = new byte[crcPosition - logEntryPosition];

        buffer.get(entryBytes);

        // Move the position as if we have read the whole payload.
        buffer.position(buffer.position() + HASH_SIZE_BYTES);

        return logEntryDecoder.decode(entryBytes);
    }

    static int size(LogEntry logEntry, LogEntryEncoder logEntryEncoder) {
        int entrySize = logEntryEncoder.size(logEntry);

        LogId logId = logEntry.getId();

        return fixedOverheadSize() + VarlenEncoder.sizeInBytes(logId.getIndex()) + VarlenEncoder.sizeInBytes(logId.getTerm()) + entrySize;
    }

    static int fixedOverheadSize() {
        return GROUP_ID_SIZE_BYTES + LENGTH_SIZE_BYTES + HASH_SIZE_BYTES;
    }
}
