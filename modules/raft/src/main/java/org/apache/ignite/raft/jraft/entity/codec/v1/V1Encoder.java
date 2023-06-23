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
import org.apache.ignite.raft.jraft.entity.EnumOutter.EntryType;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryEncoder;
import org.apache.ignite.raft.jraft.util.AsciiStringUtil;
import org.apache.ignite.raft.jraft.util.Bits;

/**
 * V1 log entry encoder
 */
public final class V1Encoder implements LogEntryEncoder {
    private V1Encoder() {
    }

    public static final LogEntryEncoder INSTANCE = new V1Encoder();

    @Override
    public byte[] encode(final LogEntry log) {
        EntryType type = log.getType();
        LogId id = log.getId();
        List<PeerId> peers = log.getPeers();
        List<PeerId> oldPeers = log.getOldPeers();
        List<PeerId> learners = log.getLearners();
        List<PeerId> oldLearners = log.getOldLearners();
        ByteBuffer data = log.getData();

        // magic number 1 byte
        int totalLen = 1;
        final int iType = type.getNumber();
        final long index = id.getIndex();
        final long term = id.getTerm();
        // type + index + term + checksum(8)
        totalLen += sizeInBytes(iType) + sizeInBytes(index) + sizeInBytes(term) + 8;

        List<String> peerStrs = null;
        int peerCount = 0;
        List<String> oldPeerStrs = null;
        int oldPeerCount = 0;
        List<String> learnerStrs = null;
        int learnerCount = 0;
        List<String> oldLearnerStrs = null;
        int oldLearnerCount = 0;

        if (type != EntryType.ENTRY_TYPE_DATA) {
            peerStrs = new ArrayList<>();
            if (peers != null) {
                peerCount = peers.size();
                for (final PeerId peer : peers) {
                    final String peerStr = peer.toString();
                    // peer len (short in 2 bytes)
                    // peer str
                    totalLen += 2 + peerStr.length();
                    peerStrs.add(peerStr);
                }
            }
            totalLen += sizeInBytes(peerCount);

            oldPeerStrs = new ArrayList<>();
            if (oldPeers != null) {
                oldPeerCount = oldPeers.size();
                for (final PeerId peer : oldPeers) {
                    final String peerStr = peer.toString();
                    // peer len (short in 2 bytes)
                    // peer str
                    totalLen += 2 + peerStr.length();
                    oldPeerStrs.add(peerStr);
                }
            }
            totalLen += sizeInBytes(oldPeerCount);

            learnerStrs = new ArrayList<>();
            if (learners != null) {
                learnerCount = learners.size();
                for (final PeerId learner : learners) {
                    final String learnerStr = learner.toString();
                    // learner len (short in 2 bytes)
                    // learner str
                    totalLen += 2 + learnerStr.length();
                    learnerStrs.add(learnerStr);
                }
            }
            totalLen += sizeInBytes(learnerCount);

            oldLearnerStrs = new ArrayList<>();
            if (oldLearners != null) {
                oldLearnerCount = oldLearners.size();
                for (final PeerId oldLearner : oldLearners) {
                    final String learnerStr = oldLearner.toString();
                    // oldLearner len (short in 2 bytes)
                    // oldLearner str
                    totalLen += 2 + learnerStr.length();
                    oldLearnerStrs.add(learnerStr);
                }
            }
            totalLen += sizeInBytes(oldLearnerCount);
        }

        if (type != EntryType.ENTRY_TYPE_CONFIGURATION) {
            final int bodyLen = data != null ? data.remaining() : 0;
            totalLen += bodyLen;
        }

        final byte[] content = new byte[totalLen];
        // {0} magic
        content[0] = LogEntryV1CodecFactory.MAGIC;
        int pos = 1;

        pos = writeLong(iType, content, pos);
        pos = writeLong(index, content, pos);
        pos = writeLong(term, content, pos);

        Bits.putLong(content, pos, log.getChecksum());
        pos += Long.BYTES;

        if (type != EntryType.ENTRY_TYPE_DATA) {
            // peer count
            pos = writeLong(peerCount, content, pos);
            // peers
            for (final String peerStr : peerStrs) {
                final byte[] ps = AsciiStringUtil.unsafeEncode(peerStr);
                Bits.putShort(content, pos, (short) peerStr.length());
                System.arraycopy(ps, 0, content, pos + 2, ps.length);
                pos += 2 + ps.length;
            }

            // old peers count
            pos = writeLong(oldPeerCount, content, pos);
            // old peers
            for (final String peerStr : oldPeerStrs) {
                final byte[] ps = AsciiStringUtil.unsafeEncode(peerStr);
                Bits.putShort(content, pos, (short) peerStr.length());
                System.arraycopy(ps, 0, content, pos + 2, ps.length);
                pos += 2 + ps.length;
            }

            // learners count
            pos = writeLong(learnerCount, content, pos);
            // learners
            for (final String peerStr : learnerStrs) {
                final byte[] ps = AsciiStringUtil.unsafeEncode(peerStr);
                Bits.putShort(content, pos, (short) peerStr.length());
                System.arraycopy(ps, 0, content, pos + 2, ps.length);
                pos += 2 + ps.length;
            }

            // old learners count
            pos = writeLong(oldPeerCount, content, pos);
            // old learners
            for (final String peerStr : oldLearnerStrs) {
                final byte[] ps = AsciiStringUtil.unsafeEncode(peerStr);
                Bits.putShort(content, pos, (short) peerStr.length());
                System.arraycopy(ps, 0, content, pos + 2, ps.length);
                pos += 2 + ps.length;
            }
        }

        if (type != EntryType.ENTRY_TYPE_CONFIGURATION) {
            // data
            if (data != null) {
                System.arraycopy(data.array(), data.position(), content, pos, data.remaining());
            }
        }

        return content;
    }

    private static int writeLong(long val, byte[] out, int pos) {
        while ((val & 0xFFFF_FFFF_FFFF_FF80L) != 0) {
            byte b = (byte) (val | 0x80);

            out[pos++] = b;

            val >>>= 7;
        }

        out[pos++] = (byte) val;

        return pos;
    }

    private static int sizeInBytes(long val) {
        if (val >= 0) {
            if (val < (1L << 7)) {
                return 1;
            } else if (val < (1L << 14)) {
                return 2;
            } else if (val < (1L << 21)) {
                return 3;
            } else if (val < (1L << 28)) {
                return 4;
            } else if (val < (1L << 35)) {
                return 5;
            } else if (val < (1L << 42)) {
                return 6;
            } else if (val < (1L << 49)) {
                return 7;
            } else if (val < (1L << 56)) {
                return 8;
            } else {
                return 9;
            }
        } else {
            return 10;
        }
    }
}