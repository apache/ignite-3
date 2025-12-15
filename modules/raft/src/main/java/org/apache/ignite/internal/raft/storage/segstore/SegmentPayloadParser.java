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

import static org.apache.ignite.internal.raft.storage.segstore.SegmentFileManager.FORMAT_VERSION;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentFileManager.MAGIC_NUMBER;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentFileManager.SWITCH_SEGMENT_RECORD;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayload.CRC_SIZE_BYTES;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayload.RESET_RECORD_MARKER;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayload.TRUNCATE_PREFIX_RECORD_MARKER;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayload.TRUNCATE_SUFFIX_RECORD_MARKER;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import org.apache.ignite.internal.raft.util.VarlenEncoder;
import org.apache.ignite.internal.util.FastCrc;

class SegmentPayloadParser {
    private final int stripes;

    SegmentPayloadParser(int stripes) {
        this.stripes = stripes;
    }

    WriteModeIndexMemTable recoverMemtable(SegmentFile segmentFile, Path segmentFilePath, boolean validateCrc) {
        ByteBuffer buffer = segmentFile.buffer();

        validateSegmentFileHeader(buffer, segmentFilePath);

        var memtable = new IndexMemTable(stripes);

        while (!endOfSegmentReached(buffer)) {
            int segmentFilePayloadOffset = buffer.position();

            long groupId = buffer.getLong();

            int payloadLength = buffer.getInt();

            int crcPosition;

            if (payloadLength == TRUNCATE_SUFFIX_RECORD_MARKER) {
                long lastLogIndexKept = buffer.getLong();

                crcPosition = buffer.position();

                buffer.position(segmentFilePayloadOffset);

                // CRC violation signals the end of meaningful data in the segment file.
                if (validateCrc && !isCrcValid(buffer, crcPosition)) {
                    break;
                }

                memtable.truncateSuffix(groupId, lastLogIndexKept);
            } else if (payloadLength == TRUNCATE_PREFIX_RECORD_MARKER) {
                long firstLogIndexKept = buffer.getLong();

                crcPosition = buffer.position();

                buffer.position(segmentFilePayloadOffset);

                // CRC violation signals the end of meaningful data in the segment file.
                if (validateCrc && !isCrcValid(buffer, crcPosition)) {
                    break;
                }

                memtable.truncatePrefix(groupId, firstLogIndexKept);
            } else if (payloadLength == RESET_RECORD_MARKER) {
                long nextLogIndex = buffer.getLong();

                crcPosition = buffer.position();

                buffer.position(segmentFilePayloadOffset);

                // CRC violation signals the end of meaningful data in the segment file.
                if (validateCrc && !isCrcValid(buffer, crcPosition)) {
                    break;
                }

                memtable.reset(groupId, nextLogIndex);
            } else {
                crcPosition = buffer.position() + payloadLength;

                long index = VarlenEncoder.readLong(buffer);

                buffer.position(segmentFilePayloadOffset);

                // CRC violation signals the end of meaningful data in the segment file.
                if (validateCrc && !isCrcValid(buffer, crcPosition)) {
                    break;
                }

                memtable.appendSegmentFileOffset(groupId, index, segmentFilePayloadOffset);
            }

            buffer.position(crcPosition + CRC_SIZE_BYTES);
        }

        return memtable;
    }

    private static void validateSegmentFileHeader(ByteBuffer buffer, Path segmentFilePath) {
        int magicNumber = buffer.getInt();

        if (magicNumber != MAGIC_NUMBER) {
            throw new IllegalStateException(String.format("Invalid magic number in segment file %s: %d.", segmentFilePath, magicNumber));
        }

        int formatVersion = buffer.getInt();

        if (formatVersion > FORMAT_VERSION) {
            throw new IllegalStateException(String.format(
                    "Unsupported format version in segment file %s: %d.", segmentFilePath, formatVersion
            ));
        }
    }

    private static boolean isCrcValid(ByteBuffer buffer, int crcPosition) {
        int originalPosition = buffer.position();

        int crc = buffer.getInt(crcPosition);

        int expectedCrc = FastCrc.calcCrc(buffer, crcPosition - buffer.position());

        buffer.position(originalPosition);

        return crc == expectedCrc;
    }

    private static boolean endOfSegmentReached(ByteBuffer buffer) {
        if (buffer.remaining() < SWITCH_SEGMENT_RECORD.length) {
            return true;
        }

        var switchSegmentRecordBytes = new byte[SWITCH_SEGMENT_RECORD.length];

        int originalPos = buffer.position();

        buffer.get(switchSegmentRecordBytes);

        buffer.position(originalPos);

        return Arrays.equals(switchSegmentRecordBytes, SWITCH_SEGMENT_RECORD);
    }
}
