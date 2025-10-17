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
package org.apache.ignite.raft.jraft.entity.codec.v1;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.raft.jraft.entity.EnumOutter;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryDecoder;
import org.apache.ignite.raft.jraft.util.AsciiStringUtil;

/**
 * V1 log entry decoder.
 */
public final class V1Decoder implements LogEntryDecoder {
    private V1Decoder() {
    }

    public static final V1Decoder INSTANCE = new V1Decoder();

    @Override
    public LogEntry decode(final byte[] content) {
        if (content == null || content.length == 0) {
            return null;
        }

        return decode(ByteBuffer.wrap(content).order(ByteOrder.LITTLE_ENDIAN));
    }

    @Override
    public LogEntry decode(ByteBuffer content) {
        if (content == null || content.remaining() == 0) {
            return null;
        }
        if (content.get() != LogEntryV1CodecFactory.MAGIC) {
            // Corrupted log
            return null;
        }
        LogEntry log = new LogEntry();
        decode(log, content);

        return log;
    }

    // Refactored to look closer to Ignites code style.
    private void decode(final LogEntry log, final ByteBuffer content) {
        int typeNumber = (int)readLong(content);
        EnumOutter.EntryType type = Objects.requireNonNull(EnumOutter.EntryType.forNumber(typeNumber));
        log.setType(type);

        long index = readLong(content);
        long term = readLong(content);
        log.setId(new LogId(index, term));

        long checksum = content.getLong();
        log.setChecksum(checksum);

        // Peers and learners.
        if (type != EnumOutter.EntryType.ENTRY_TYPE_DATA) {
            int peerCount = (int)readLong(content);
            if (peerCount > 0) {
                List<PeerId> peers = new ArrayList<>(peerCount);

                readNodesList(content, peerCount, peers);

                log.setPeers(peers);
            }

            int oldPeerCount = (int)readLong(content);
            if (oldPeerCount > 0) {
                List<PeerId> oldPeers = new ArrayList<>(oldPeerCount);

                readNodesList(content, oldPeerCount, oldPeers);

                log.setOldPeers(oldPeers);
            }

            int learnersCount = (int)readLong(content);
            if (learnersCount > 0) {
                List<PeerId> learners = new ArrayList<>(learnersCount);

                readNodesList(content, learnersCount, learners);

                log.setLearners(learners);
            }

            int oldLearnersCount = (int)readLong(content);
            if (oldLearnersCount > 0) {
                List<PeerId> oldLearners = new ArrayList<>(oldLearnersCount);

                readNodesList(content, oldLearnersCount, oldLearners);

                log.setOldLearners(oldLearners);
            }
        }

        // Data.
        if (type != EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
            if (content.remaining() > 0) {
                log.setData(content.slice());
            }
        }
    }

    private static void readNodesList(ByteBuffer content, int count, List<PeerId> nodes) {
        for (int i = 0; i < count; i++) {
            short len = content.getShort();

            String consistentId = AsciiStringUtil.unsafeDecode(content, len);

            int idx = (int) readLong(content);
            int priority = (int) (readLong(content) - 1);

            nodes.add(new PeerId(consistentId, idx, priority));
        }
    }

    private static long readLong(ByteBuffer content) {
        long val = 0;
        int shift = 0;

        while (true) {
            byte b = content.get();

            val |= ((long) b & 0x7F) << shift;

            if ((b & 0x80) == 0) {
                return val;
            } else {
                shift += 7;
            }
        }
    }
}
