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

    public void decode(final LogEntry log, final byte[] content) {
        var reader = new Reader(content);
        reader.pos = 1;

        // type
        final int iType = (int)reader.readLong();
        EnumOutter.EntryType type = EnumOutter.EntryType.forNumber(iType);
        log.setType(type);
        // index
        // term
        final long index = reader.readLong();
        final long term = reader.readLong();
        log.setId(new LogId(index, term));
        // checksum
        log.setChecksum(Bits.getLong(content, reader.pos));
        int pos = reader.pos + Long.BYTES;

        if (type != EnumOutter.EntryType.ENTRY_TYPE_DATA) {
            // peer count
            reader.pos = pos;
            int peerCount = (int)reader.readLong();
            pos = reader.pos;

            // peers
            if (peerCount > 0) {
                List<PeerId> peers = new ArrayList<>(peerCount);
                while (peerCount-- > 0) {
                    final short len = Bits.getShort(content, pos);
                    final byte[] bs = new byte[len];
                    System.arraycopy(content, pos + 2, bs, 0, len);
                    // peer len (short in 2 bytes)
                    // peer str
                    pos += 2 + len;
                    final PeerId peer = new PeerId();
                    peer.parse(AsciiStringUtil.unsafeDecode(bs));
                    peers.add(peer);
                }
                log.setPeers(peers);
            }
            // old peers
            reader.pos = pos;
            int oldPeerCount = (int)reader.readLong();
            pos = reader.pos;
            if (oldPeerCount > 0) {
                List<PeerId> oldPeers = new ArrayList<>(oldPeerCount);
                while (oldPeerCount-- > 0) {
                    final short len = Bits.getShort(content, pos);
                    final byte[] bs = new byte[len];
                    System.arraycopy(content, pos + 2, bs, 0, len);
                    // peer len (short in 2 bytes)
                    // peer str
                    pos += 2 + len;
                    final PeerId peer = new PeerId();
                    peer.parse(AsciiStringUtil.unsafeDecode(bs));
                    oldPeers.add(peer);
                }
                log.setOldPeers(oldPeers);
            }
            // learners
            reader.pos = pos;
            int learnersCount = (int)reader.readLong();
            pos = reader.pos;
            if (learnersCount > 0) {
                List<PeerId> learners = new ArrayList<>(learnersCount);
                while (learnersCount-- > 0) {
                    final short len = Bits.getShort(content, pos);
                    final byte[] bs = new byte[len];
                    System.arraycopy(content, pos + 2, bs, 0, len);
                    // peer len (short in 2 bytes)
                    // peer str
                    pos += 2 + len;
                    final PeerId peer = new PeerId();
                    peer.parse(AsciiStringUtil.unsafeDecode(bs));
                    learners.add(peer);
                }
                log.setLearners(learners);
            }
            // old learners
            reader.pos = pos;
            int oldLearnersCount = (int)reader.readLong();
            pos = reader.pos;
            if (oldLearnersCount > 0) {
                List<PeerId> oldLearners = new ArrayList<>(oldLearnersCount);
                while (oldLearnersCount-- > 0) {
                    final short len = Bits.getShort(content, pos);
                    final byte[] bs = new byte[len];
                    System.arraycopy(content, pos + 2, bs, 0, len);
                    // peer len (short in 2 bytes)
                    // peer str
                    pos += 2 + len;
                    final PeerId peer = new PeerId();
                    peer.parse(AsciiStringUtil.unsafeDecode(bs));
                    oldLearners.add(peer);
                }
                log.setOldLearners(oldLearners);
            }
        }

        if (type != EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
            // data
            if (content.length > pos) {
                final int len = content.length - pos;
                ByteBuffer data = ByteBuffer.allocate(len);
                data.put(content, pos, len);
                data.flip();
                log.setData(data);
            }
        }
    }

    private static class Reader {
        private final byte[] content;
        int pos;

        private Reader(byte[] content) {
            this.content = content;
        }

        public long readLong() {
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