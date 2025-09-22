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
import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayload.HASH_SIZE;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayload.LENGTH_SIZE_BYTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
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

    static DeserializedSegmentPayload fromBytes(byte[] bytes) {
        return fromByteBuffer(ByteBuffer.wrap(bytes).order(SegmentFile.BYTE_ORDER));
    }

    static DeserializedSegmentPayload fromByteBuffer(ByteBuffer entryBuf) {
        long groupId = entryBuf.getLong();

        int payloadLength = entryBuf.getInt();

        byte[] payload = new byte[payloadLength];

        entryBuf.get(payload);

        int entrySizeWithoutCrc = entryBuf.position();
        int actualCrc = entryBuf.getInt();
        int expectedCrc = FastCrc.calcCrc(entryBuf.rewind(), entrySizeWithoutCrc);

        assertThat(actualCrc, is(expectedCrc));

        return new DeserializedSegmentPayload(groupId, payload);
    }

    static @Nullable DeserializedSegmentPayload fromInputStream(InputStream is) throws IOException {
        byte[] groupIdBytes = is.readNBytes(GROUP_ID_SIZE_BYTES);

        if (groupIdBytes.length < GROUP_ID_SIZE_BYTES) {
            // EOF reached.
            return null;
        }

        long groupId = ByteBuffer.wrap(groupIdBytes).order(SegmentFile.BYTE_ORDER).getLong();

        if (groupId == 0) {
            // EOF reached.
            return null;
        }

        int payloadLength = ByteBuffer.wrap(is.readNBytes(LENGTH_SIZE_BYTES)).order(SegmentFile.BYTE_ORDER).getInt();

        byte[] remaining = is.readNBytes(payloadLength + HASH_SIZE);

        ByteBuffer entry = ByteBuffer.allocate(GROUP_ID_SIZE_BYTES + LENGTH_SIZE_BYTES + payloadLength + HASH_SIZE)
                .order(SegmentFile.BYTE_ORDER)
                .putLong(groupId)
                .putInt(payloadLength)
                .put(remaining)
                .flip();

        return fromByteBuffer(entry);
    }

    long groupId() {
        return groupId;
    }

    byte[] payload() {
        return payload;
    }

    int size() {
        return GROUP_ID_SIZE_BYTES + LENGTH_SIZE_BYTES + payload.length + HASH_SIZE;
    }
}
