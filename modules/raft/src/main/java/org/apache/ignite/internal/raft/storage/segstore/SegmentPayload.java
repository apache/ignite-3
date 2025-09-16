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
import org.apache.ignite.internal.util.FastCrc;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryEncoder;

/**
 * Describes a payload entry in a segment file.
 *
 * @see SegmentFileManager
 */
class SegmentPayload {
    static final int GROUP_ID_SIZE_BYTES = Long.BYTES;

    static final int LENGTH_SIZE_BYTES = Integer.BYTES;

    static final int HASH_SIZE = Integer.BYTES;

    private final long groupId;

    private final int payloadSize;

    private final LogEntry logEntry;

    private final LogEntryEncoder logEntryEncoder;

    SegmentPayload(long groupId, LogEntry logEntry, LogEntryEncoder logEntryEncoder) {
        this.groupId = groupId;
        this.logEntry = logEntry;
        this.logEntryEncoder = logEntryEncoder;

        payloadSize = logEntryEncoder.size(logEntry);
    }

    void writeTo(ByteBuffer buffer) {
        int originalPos = buffer.position();

        buffer
                .putLong(groupId)
                .putInt(payloadSize);

        logEntryEncoder.encode(buffer, logEntry);

        int dataSize = buffer.position() - originalPos;

        // Rewind the position for CRC calculation.
        buffer.position(originalPos);

        int crc = FastCrc.calcCrc(buffer, dataSize);

        // After CRC calculation the position will be at the provided end of the buffer.
        buffer.putInt(crc);
    }

    int size() {
        return overheadSize() + payloadSize;
    }

    static int overheadSize() {
        return GROUP_ID_SIZE_BYTES + LENGTH_SIZE_BYTES + HASH_SIZE;
    }
}
