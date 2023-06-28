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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.raft.jraft.entity.EnumOutter;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryDecoder;
import org.apache.ignite.raft.jraft.util.AsciiStringUtil;
import org.apache.ignite.raft.jraft.util.Bits;

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
        if (content[0] != LogEntryV1CodecFactory.MAGIC) {
            // Corrupted log
            return null;
        }
        LogEntry log = new LogEntry();
        decode(log, content);

        return log;
    }

    // Refactored to look closer to Ignites code style.
    public void decode(final LogEntry log, final byte[] content) {
        var reader = new Reader(content);
        reader.pos = LogEntryV1CodecFactory.PAYLOAD_OFFSET;

        int typeNumber = (int)reader.readLong();
        EnumOutter.EntryType type = Objects.requireNonNull(EnumOutter.EntryType.forNumber(typeNumber));
        log.setType(type);

        long index = reader.readLong();
        long term = reader.readLong();
        log.setId(new LogId(index, term));

        long checksum = Bits.getLong(content, reader.pos);
        log.setChecksum(checksum);

        int pos = reader.pos + Long.BYTES;

        // Peers and learners.
        if (type != EnumOutter.EntryType.ENTRY_TYPE_DATA) {
            reader.pos = pos;
            int peerCount = (int)reader.readLong();
            pos = reader.pos;
            if (peerCount > 0) {
                List<PeerId> peers = new ArrayList<>(peerCount);

                pos = readNodesList(pos, content, peerCount, peers);

                log.setPeers(peers);
            }

            reader.pos = pos;
            int oldPeerCount = (int)reader.readLong();
            pos = reader.pos;
            if (oldPeerCount > 0) {
                List<PeerId> oldPeers = new ArrayList<>(oldPeerCount);

                pos = readNodesList(pos, content, oldPeerCount, oldPeers);

                log.setOldPeers(oldPeers);
            }

            reader.pos = pos;
            int learnersCount = (int)reader.readLong();
            pos = reader.pos;
            if (learnersCount > 0) {
                List<PeerId> learners = new ArrayList<>(learnersCount);

                pos = readNodesList(pos, content, learnersCount, learners);

                log.setLearners(learners);
            }

            reader.pos = pos;
            int oldLearnersCount = (int)reader.readLong();
            pos = reader.pos;
            if (oldLearnersCount > 0) {
                List<PeerId> oldLearners = new ArrayList<>(oldLearnersCount);

                pos = readNodesList(pos, content, oldLearnersCount, oldLearners);

                log.setOldLearners(oldLearners);
            }
        }

        // Data.
        if (type != EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
            if (content.length > pos) {
                int len = content.length - pos;

                ByteBuffer data = ByteBuffer.wrap(content, pos, len).slice();

                log.setData(data);
            }
        }
    }

    private static int readNodesList(int pos, byte[] content, int count, List<PeerId> nodes) {
        for (int i = 0; i < count; i++) {
            short len = Bits.getShort(content, pos);
            pos += 2;

            PeerId peer = new PeerId();
            peer.parse(AsciiStringUtil.unsafeDecode(content, pos, len));
            nodes.add(peer);

            pos += len;
        }

        return pos;
    }

    /*
     * Allows reading varlen numbers and tracking position at the same time. Simply having a "readLong" method wasn't enough.
     */
    private static class Reader {
        private final byte[] content;
        int pos;

        private Reader(byte[] content) {
            this.content = content;
        }

        // Based on DirectByteBufferStreamImplV1.
        long readLong() {
            long val = 0;
            int shift = 0;

            while (true) {
                byte b = content[pos];

                pos++;

                val |= ((long) b & 0x7F) << shift;

                if ((b & 0x80) == 0) {
                    return val;
                } else {
                    shift += 7;
                }
            }
        }
    }
}