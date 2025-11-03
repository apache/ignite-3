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

import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayload.GROUP_ID_SIZE_BYTES;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayload.HASH_SIZE_BYTES;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayload.LENGTH_SIZE_BYTES;
import static org.apache.ignite.internal.raft.util.VarlenEncoder.readLong;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import org.apache.ignite.internal.util.FastCrc;
import org.jetbrains.annotations.Nullable;

/**
 * Test-only class representing a partially deserialized {@link SegmentPayload}.
 */
class DeserializedSegmentPayload {
    private final long groupId;

    private final byte[] payload;

    private DeserializedSegmentPayload(long groupId, byte[] payload) {
        this.groupId = groupId;
        this.payload = payload;
    }

    static @Nullable DeserializedSegmentPayload fromByteChannel(ReadableByteChannel channel) throws IOException {
        ByteBuffer groupIdBytes;

        try {
            groupIdBytes = readFully(channel, GROUP_ID_SIZE_BYTES);
        } catch (EOFException e) {
            // EOF reached.
            return null;
        }

        long groupId = groupIdBytes.getLong();

        if (groupId == 0) {
            // EOF reached.
            return null;
        }

        int payloadLength = readFully(channel, LENGTH_SIZE_BYTES).getInt();

        ByteBuffer remaining = readFully(channel, payloadLength + HASH_SIZE_BYTES);

        ByteBuffer fullEntry = ByteBuffer.allocate(payloadLength + GROUP_ID_SIZE_BYTES + LENGTH_SIZE_BYTES + HASH_SIZE_BYTES)
                .order(SegmentFile.BYTE_ORDER)
                .putLong(groupId)
                .putInt(payloadLength)
                .put(remaining)
                .rewind();

        int crcOffset = payloadLength + GROUP_ID_SIZE_BYTES + LENGTH_SIZE_BYTES;

        int actualCrc = fullEntry.getInt(crcOffset);

        int expectedCrc = FastCrc.calcCrc(fullEntry, crcOffset);

        assertThat(actualCrc, is(expectedCrc));

        fullEntry.rewind();

        return fromByteBuffer(fullEntry);
    }

    private static DeserializedSegmentPayload fromByteBuffer(ByteBuffer entryBuf) {
        long groupId = entryBuf.getLong();

        int payloadLength = entryBuf.getInt();

        int pos = entryBuf.position();

        readLong(entryBuf); // Skip log entry index.
        readLong(entryBuf); // Skip log entry term.

        int indexAndTermSize = entryBuf.position() - pos;

        byte[] payload = new byte[payloadLength - indexAndTermSize];

        entryBuf.get(payload);

        return new DeserializedSegmentPayload(groupId, payload);
    }

    long groupId() {
        return groupId;
    }

    byte[] payload() {
        return payload;
    }

    int size() {
        return GROUP_ID_SIZE_BYTES + LENGTH_SIZE_BYTES + payload.length + HASH_SIZE_BYTES;
    }

    private static ByteBuffer readFully(ReadableByteChannel byteChannel, int len) throws IOException {
        return ByteChannelUtils.readFully(byteChannel, len).order(SegmentFile.BYTE_ORDER);
    }
}
