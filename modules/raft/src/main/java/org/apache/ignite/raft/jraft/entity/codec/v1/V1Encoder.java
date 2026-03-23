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

import static org.apache.ignite.internal.raft.util.VarlenEncoder.sizeInBytes;
import static org.apache.ignite.internal.raft.util.VarlenEncoder.writeLong;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
    @Override
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

        addr += writeLong(typeNumber, addr);
        addr += writeLong(index, addr);
        addr += writeLong(term, addr);

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
        int totalLen = size(log);

        ByteBuffer buffer = ByteBuffer.allocate(totalLen).order(ByteOrder.LITTLE_ENDIAN);

        encode(buffer, log);

        return buffer.array();
    }

    @Override
    public void encode(ByteBuffer buffer, LogEntry log) {
        EntryType type = log.getType();
        LogId id = log.getId();
        List<PeerId> peers = log.getPeers();
        List<PeerId> oldPeers = log.getOldPeers();
        List<PeerId> learners = log.getLearners();
        List<PeerId> oldLearners = log.getOldLearners();
        ByteBuffer data = log.getReadOnlyData();

        int typeNumber = type.getNumber();
        long index = id.getIndex();
        long term = id.getTerm();

        buffer.put(LogEntryV1CodecFactory.MAGIC);

        writeLong(typeNumber, buffer);
        writeLong(index, buffer);
        writeLong(term, buffer);

        buffer.putLong(log.getChecksum());

        if (type != EntryType.ENTRY_TYPE_DATA) {
            writeNodesList(buffer, peers);

            writeNodesList(buffer, oldPeers);

            writeNodesList(buffer, learners);

            writeNodesList(buffer, oldLearners);
        }

        if (type != EntryType.ENTRY_TYPE_CONFIGURATION && data != null) {
            buffer.put(data);
        }
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
            return addr + writeLong(0, addr);
        }

        addr += writeLong(nodes.size(), addr);

        for (PeerId node : nodes) {
            String nodeStr = node.getConsistentId();
            int length = nodeStr.length();

            Bits.putShortLittleEndian(addr, (short) length);
            addr += Short.BYTES;

            for (int i = 0; i < length; i++) {
                GridUnsafe.putByte(addr + i, (byte) nodeStr.charAt(i));
            }
            addr += length;

            addr += writeLong(node.getIdx(), addr);
            addr += writeLong(node.getPriority() + 1, addr);
        }

        return addr;
    }

    private static void writeNodesList(ByteBuffer content, List<PeerId> nodeStrs) {
        if (nodeStrs == null) {
            content.put((byte) 0);

            return;
        }

        writeLong(nodeStrs.size(), content);

        for (PeerId peerId : nodeStrs) {
            String consistentId = peerId.getConsistentId();
            int length = consistentId.length();

            content.putShort((short) length);

            AsciiStringUtil.unsafeEncode(consistentId, content);

            writeLong(peerId.getIdx(), content);
            writeLong(peerId.getPriority() + 1, content);
        }
    }
}
