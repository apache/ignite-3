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
import org.jetbrains.annotations.Nullable;

/**
 * V1 log entry encoder
 */
public final class V1Encoder implements LogEntryEncoder {
    private V1Encoder() {
    }

    public static final LogEntryEncoder INSTANCE = new V1Encoder();

    // Refactored to look closer to Ignites code style.
    @Override
    public byte[] encode(final LogEntry log) {
        EntryType type = log.getType();
        LogId id = log.getId();
        List<PeerId> peers = log.getPeers();
        List<PeerId> oldPeers = log.getOldPeers();
        List<PeerId> learners = log.getLearners();
        List<PeerId> oldLearners = log.getOldLearners();
        ByteBuffer data = log.getData();

        int totalLen = LogEntryV1CodecFactory.PAYLOAD_OFFSET;
        int typeNumber = type.getNumber();
        long index = id.getIndex();
        long term = id.getTerm();

        totalLen += sizeInBytes(typeNumber) + sizeInBytes(index) + sizeInBytes(term) + 8;

        List<String> peerStrs = null;
        List<String> oldPeerStrs = null;
        List<String> learnerStrs = null;
        List<String> oldLearnerStrs = null;

        if (type != EntryType.ENTRY_TYPE_DATA) {
            peerStrs = new ArrayList<>();
            totalLen += nodesListSizeInBytes(peers, peerStrs);

            oldPeerStrs = new ArrayList<>();
            totalLen += nodesListSizeInBytes(oldPeers, oldPeerStrs);

            learnerStrs = new ArrayList<>();
            totalLen += nodesListSizeInBytes(learners, learnerStrs);

            oldLearnerStrs = new ArrayList<>();
            totalLen += nodesListSizeInBytes(oldLearners, oldLearnerStrs);
        }

        if (type != EntryType.ENTRY_TYPE_CONFIGURATION) {
            int bodyLen = data != null ? data.remaining() : 0;
            totalLen += bodyLen;
        }

        byte[] content = new byte[totalLen];
        content[0] = LogEntryV1CodecFactory.MAGIC;
        int pos = LogEntryV1CodecFactory.PAYLOAD_OFFSET;

        pos = writeLong(typeNumber, content, pos);
        pos = writeLong(index, content, pos);
        pos = writeLong(term, content, pos);

        Bits.putLong(content, pos, log.getChecksum());
        pos += Long.BYTES;

        if (type != EntryType.ENTRY_TYPE_DATA) {
            pos = writeNodesList(pos, content, peerStrs);

            pos = writeNodesList(pos, content, oldPeerStrs);

            pos = writeNodesList(pos, content, learnerStrs);

            pos = writeNodesList(pos, content, oldLearnerStrs);
        }

        if (type != EntryType.ENTRY_TYPE_CONFIGURATION && data != null) {
            System.arraycopy(data.array(), data.position(), content, pos, data.remaining());
        }

        return content;
    }

    private static int nodesListSizeInBytes(@Nullable List<PeerId> nodes, List<String> nodeStrs) {
        int size = 0;

        if (nodes != null) {
            for (PeerId node : nodes) {
                String nodeStr = node.toString();

                nodeStrs.add(nodeStr);
                size += 2 + nodeStr.length();
            }
        }

        return size + sizeInBytes(nodeStrs.size());
    }

    private static int writeNodesList(int pos, byte[] content, List<String> nodeStrs) {
        pos = writeLong(nodeStrs.size(), content, pos);

        for (String nodeStr : nodeStrs) {
            int length = nodeStr.length();

            Bits.putShort(content, pos, (short) length);
            pos += 2;

            AsciiStringUtil.unsafeEncode(nodeStr, content, pos);
            pos += length;
        }

        return pos;
    }

    // Based on DirectByteBufferStreamImplV1.
    private static int writeLong(long val, byte[] out, int pos) {
        while ((val & 0xFFFF_FFFF_FFFF_FF80L) != 0) {
            byte b = (byte) (val | 0x80);

            out[pos++] = b;

            val >>>= 7;
        }

        out[pos++] = (byte) val;

        return pos;
    }

    /**
     * Returns the number of bytes, required by the {@link #writeLong(long, byte[], int)} to write the value.
    */
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