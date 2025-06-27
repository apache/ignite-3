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
import java.util.List;
import org.apache.ignite.internal.util.GridUnsafe;
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

    public static final V1Encoder INSTANCE = new V1Encoder();

    /**
     * Returns a size of an encoded entry. Must match the size of {@link #encode(LogEntry)} output.
     *
     * @param logEntry Log entry.
     */
    public int size(LogEntry logEntry) {
        EntryType type = logEntry.getType();
        LogId id = logEntry.getId();

        List<PeerId> peers = logEntry.getPeers();
        List<PeerId> oldPeers = logEntry.getOldPeers();
        List<PeerId> learners = logEntry.getLearners();
        List<PeerId> oldLearners = logEntry.getOldLearners();

        ByteBuffer data = logEntry.getData();

        int totalLen = LogEntryV1CodecFactory.PAYLOAD_OFFSET;
        int typeNumber = type.getNumber();
        long index = id.getIndex();
        long term = id.getTerm();

        // Checksum is not a varlen value.
        totalLen += sizeInBytes(typeNumber) + sizeInBytes(index) + sizeInBytes(term) + Long.BYTES;

        // Includes "ENTRY_TYPE_CONFIGURATION" and "ENTRY_TYPE_NO_OP".
        if (type != EntryType.ENTRY_TYPE_DATA) {
            totalLen += nodesListSizeInBytes(peers);
            totalLen += nodesListSizeInBytes(oldPeers);
            totalLen += nodesListSizeInBytes(learners);
            totalLen += nodesListSizeInBytes(oldLearners);
        }

        // Includes "ENTRY_TYPE_DATA" and "ENTRY_TYPE_NO_OP".
        if (type != EntryType.ENTRY_TYPE_CONFIGURATION) {
            int bodyLen = data != null ? data.remaining() : 0;
            totalLen += bodyLen;
        }

        return totalLen;
    }

    /**
     * Writes the same data as {@link #encode(LogEntry)} directly into a given address.
     *
     * @param addr Off-heap address.
     * @param logEntry Log entry.
     */
    public void append(long addr, LogEntry logEntry) {
        EntryType type = logEntry.getType();
        LogId id = logEntry.getId();

        List<PeerId> peers = logEntry.getPeers();
        List<PeerId> oldPeers = logEntry.getOldPeers();
        List<PeerId> learners = logEntry.getLearners();
        List<PeerId> oldLearners = logEntry.getOldLearners();

        ByteBuffer data = logEntry.getData();

        int typeNumber = type.getNumber();
        long index = id.getIndex();
        long term = id.getTerm();

        GridUnsafe.putByte(addr++, LogEntryV1CodecFactory.MAGIC);

        addr = writeLong(typeNumber, addr);
        addr = writeLong(index, addr);
        addr = writeLong(term, addr);

        Bits.putLongLittleEndian(addr, logEntry.getChecksum());
        addr += Long.BYTES;

        // Includes "ENTRY_TYPE_CONFIGURATION" and "ENTRY_TYPE_NO_OP".
        if (type != EntryType.ENTRY_TYPE_DATA) {
            addr = writeNodesList(addr, peers);
            addr = writeNodesList(addr, oldPeers);
            addr = writeNodesList(addr, learners);
            addr = writeNodesList(addr, oldLearners);
        }

        // Includes "ENTRY_TYPE_DATA" and "ENTRY_TYPE_NO_OP".
        if (type != EntryType.ENTRY_TYPE_CONFIGURATION && data != null) {
            GridUnsafe.copyHeapOffheap(data.array(), data.position() + GridUnsafe.BYTE_ARR_OFF, addr, data.remaining());
        }
    }

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

        int typeNumber = type.getNumber();
        long index = id.getIndex();
        long term = id.getTerm();

        int totalLen = size(log);

        byte[] content = new byte[totalLen];
        content[0] = LogEntryV1CodecFactory.MAGIC;
        int pos = LogEntryV1CodecFactory.PAYLOAD_OFFSET;

        pos = writeLong(typeNumber, content, pos);
        pos = writeLong(index, content, pos);
        pos = writeLong(term, content, pos);

        Bits.putLongLittleEndian(content, pos, log.getChecksum());
        pos += Long.BYTES;

        if (type != EntryType.ENTRY_TYPE_DATA) {
            pos = writeNodesList(pos, content, peers);

            pos = writeNodesList(pos, content, oldPeers);

            pos = writeNodesList(pos, content, learners);

            pos = writeNodesList(pos, content, oldLearners);
        }

        if (type != EntryType.ENTRY_TYPE_CONFIGURATION && data != null) {
            System.arraycopy(data.array(), data.position(), content, pos, data.remaining());
        }

        return content;
    }

    private static int nodesListSizeInBytes(@Nullable List<PeerId> nodes) {
        if (nodes == null) {
            // The size of encoded "0".
            return 1;
        }

        int size = 0;

        for (PeerId node : nodes) {
            String consistentId = node.getConsistentId();

            size += Short.BYTES + consistentId.length() + sizeInBytes(node.getIdx()) + sizeInBytes(node.getPriority() + 1);
        }

        return size + sizeInBytes(nodes.size());
    }

    private static long writeNodesList(long addr, List<PeerId> nodes) {
        if (nodes == null) {
            return writeLong(0, addr);
        }

        addr = writeLong(nodes.size(), addr);

        for (PeerId node : nodes) {
            String nodeStr = node.getConsistentId();
            int length = nodeStr.length();

            Bits.putShortLittleEndian(addr, (short) length);
            addr += Short.BYTES;

            for (int i = 0; i < length; i++) {
                GridUnsafe.putByte(addr + i, (byte) nodeStr.charAt(i));
            }
            addr += length;

            addr = writeLong(node.getIdx(), addr);
            addr = writeLong(node.getPriority() + 1, addr);
        }

        return addr;
    }

    private static int writeNodesList(int pos, byte[] content, List<PeerId> nodeStrs) {
        if (nodeStrs == null) {
            content[pos] = 0;

            return pos + 1;
        }

        pos = writeLong(nodeStrs.size(), content, pos);

        for (PeerId peerId : nodeStrs) {
            String consistentId = peerId.getConsistentId();
            int length = consistentId.length();

            Bits.putShortLittleEndian(content, pos, (short) length);
            pos += Short.BYTES;

            AsciiStringUtil.unsafeEncode(consistentId, content, pos);
            pos += length;

            pos = writeLong(peerId.getIdx(), content, pos);
            pos = writeLong(peerId.getPriority() + 1, content, pos);
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

    private static long writeLong(long val, long addr) {
        while ((val & 0xFFFF_FFFF_FFFF_FF80L) != 0) {
            byte b = (byte) (val | 0x80);

            GridUnsafe.putByte(addr++, b);

            val >>>= 7;
        }

        GridUnsafe.putByte(addr++, (byte) val);

        return addr;
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