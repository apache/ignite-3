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
package org.apache.ignite.raft.jraft.entity.codec.v2;

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
 * V2 log entry decoder. Extends V1 with sequence token support.
 */
public final class V2Decoder implements LogEntryDecoder {
    private V2Decoder() {
    }

    public static final V2Decoder INSTANCE = new V2Decoder();

    @Override
    public LogEntry decode(final byte[] content) {
        if (content == null || content.length == 0) {
            return null;
        }
        if (content[0] != LogEntryV2CodecFactory.MAGIC) {
            // Corrupted log or wrong version
            return null;
        }
        LogEntry log = new LogEntry();
        decode(log, content);

        return log;
    }

    private void decode(final LogEntry log, final byte[] content) {
        var reader = new Reader(content);
        reader.pos = LogEntryV2CodecFactory.PAYLOAD_OFFSET;

        int typeNumber = (int)reader.readLong();
        EnumOutter.EntryType type = Objects.requireNonNull(EnumOutter.EntryType.forNumber(typeNumber));
        log.setType(type);

        long index = reader.readLong();
        long term = reader.readLong();
        log.setId(new LogId(index, term));

        long checksum = Bits.getLongLittleEndian(content, reader.pos);
        log.setChecksum(checksum);

        int pos = reader.pos + Long.BYTES;

        // V2: Read sequence tokens
        reader.pos = pos;
        long sequenceToken = reader.readLong();
        log.setSequenceToken(sequenceToken);

        long oldSequenceToken = reader.readLong();
        log.setOldSequenceToken(oldSequenceToken);
        pos = reader.pos;

        // Peers and learners.
        if (type != EnumOutter.EntryType.ENTRY_TYPE_DATA) {
            reader.pos = pos;
            int peerCount = (int)reader.readLong();
            pos = reader.pos;
            if (peerCount > 0) {
                List<PeerId> peers = new ArrayList<>(peerCount);

                pos = readNodesList(reader, pos, content, peerCount, peers);

                log.setPeers(peers);
            }

            reader.pos = pos;
            int oldPeerCount = (int)reader.readLong();
            pos = reader.pos;
            if (oldPeerCount > 0) {
                List<PeerId> oldPeers = new ArrayList<>(oldPeerCount);

                pos = readNodesList(reader, pos, content, oldPeerCount, oldPeers);

                log.setOldPeers(oldPeers);
            }

            reader.pos = pos;
            int learnersCount = (int)reader.readLong();
            pos = reader.pos;
            if (learnersCount > 0) {
                List<PeerId> learners = new ArrayList<>(learnersCount);

                pos = readNodesList(reader, pos, content, learnersCount, learners);

                log.setLearners(learners);
            }

            reader.pos = pos;
            int oldLearnersCount = (int)reader.readLong();
            pos = reader.pos;
            if (oldLearnersCount > 0) {
                List<PeerId> oldLearners = new ArrayList<>(oldLearnersCount);

                pos = readNodesList(reader, pos, content, oldLearnersCount, oldLearners);

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

    private static int readNodesList(Reader reader, int pos, byte[] content, int count, List<PeerId> nodes) {
        for (int i = 0; i < count; i++) {
            short len = Bits.getShortLittleEndian(content, pos);
            pos += Short.BYTES;

            String consistentId = AsciiStringUtil.unsafeDecode(content, pos, len);
            pos += len;

            reader.pos = pos;
            int idx = (int) reader.readLong();
            int priority = (int) (reader.readLong() - 1);
            pos = reader.pos;

            nodes.add(new PeerId(consistentId, idx, priority));
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
