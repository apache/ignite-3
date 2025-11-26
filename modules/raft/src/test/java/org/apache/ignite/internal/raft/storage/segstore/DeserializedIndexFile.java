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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.raft.storage.segstore.IndexFileManager.COMMON_META_SIZE;
import static org.apache.ignite.internal.raft.storage.segstore.IndexFileManager.FORMAT_VERSION;
import static org.apache.ignite.internal.raft.storage.segstore.IndexFileManager.GROUP_META_SIZE;
import static org.apache.ignite.internal.raft.storage.segstore.IndexFileManager.MAGIC_NUMBER;
import static org.apache.ignite.internal.util.IgniteUtils.newHashMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

/**
 * Test-only class representing a deserialized index file.
 */
class DeserializedIndexFile {
    /** groupId -> logIndex -> segmentFileOffset. */
    private final Map<Long, Map<Long, Integer>> content;

    private DeserializedIndexFile(Map<Long, Map<Long, Integer>> content) {
        this.content = content;
    }

    static DeserializedIndexFile fromFile(Path path) throws IOException {
        try (SeekableByteChannel channel = Files.newByteChannel(path)) {
            // Read common meta part of the header.
            ByteBuffer commonMeta = readFully(channel, COMMON_META_SIZE);

            assertThat(commonMeta.getInt(), is(MAGIC_NUMBER));
            assertThat(commonMeta.getInt(), is(FORMAT_VERSION));

            int numGroups = commonMeta.getInt();

            // groupId -> logIndex -> segmentFileOffset.
            Map<Long, Map<Long, Integer>> content = newHashMap(numGroups);

            for (int i = 0; i < numGroups; i++) {
                // Read group meta part of the header.
                ByteBuffer groupMeta = readFully(channel, GROUP_META_SIZE);

                long groupId = groupMeta.getLong();

                assertThat(groupMeta.getInt(), is(0));

                int indexPayloadOffset = groupMeta.getInt();

                long firstIndex = groupMeta.getLong();

                long lastIndex = groupMeta.getLong();

                // Read the payload of the group.
                int payloadEntriesNum = (int) (lastIndex - firstIndex);

                Map<Long, Integer> logIndexToSegmentFileOffset = newHashMap(payloadEntriesNum);

                content.put(groupId, logIndexToSegmentFileOffset);

                long currentPosition = channel.position();

                channel.position(indexPayloadOffset);

                ByteBuffer indexPayload = readFully(channel, Integer.BYTES * payloadEntriesNum);

                channel.position(currentPosition);

                for (long logIndex = firstIndex; logIndex < lastIndex; logIndex++) {
                    logIndexToSegmentFileOffset.put(logIndex, indexPayload.getInt());
                }
            }

            return new DeserializedIndexFile(content);
        }
    }

    @Nullable
    Integer getSegmentFileOffset(long groupId, long logIndex) {
        Map<Long, Integer> logIndexToSegmentFileOffset = content.get(groupId);

        return logIndexToSegmentFileOffset == null ? null : logIndexToSegmentFileOffset.get(logIndex);
    }

    List<Entry> entries() {
        return content.entrySet().stream()
                .flatMap(e -> {
                    long groupId = e.getKey();

                    Map<Long, Integer> logIndexToSegmentFileOffset = e.getValue();

                    return logIndexToSegmentFileOffset.entrySet().stream()
                            .map(e2 -> {
                                long logIndex = e2.getKey();

                                int segmentFileOffset = e2.getValue();

                                return new Entry(groupId, logIndex, segmentFileOffset);
                            });
                })
                .collect(toList());
    }

    private static ByteBuffer readFully(ReadableByteChannel byteChannel, int len) throws IOException {
        return ByteChannelUtils.readFully(byteChannel, len).order(IndexFileManager.BYTE_ORDER);
    }

    static class Entry {
        private final long groupId;
        private final long logIndex;
        private final int segmentFileOffset;

        Entry(long groupId, long logIndex, int segmentFileOffset) {
            this.groupId = groupId;
            this.logIndex = logIndex;
            this.segmentFileOffset = segmentFileOffset;
        }

        long groupId() {
            return groupId;
        }

        long logIndex() {
            return logIndex;
        }

        int segmentFileOffset() {
            return segmentFileOffset;
        }
    }
}
