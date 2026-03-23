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

package org.apache.ignite.raft.jraft.storage.logit.storage.file.segment;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.codec.DefaultLogEntryCodecFactory;
import org.apache.ignite.raft.jraft.entity.codec.v1.V1Encoder;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.logit.storage.file.AbstractFile;
import org.apache.ignite.raft.jraft.util.Bits;

/**
 *  * File header:
 *  * <pre>
 *  *   magic bytes       first log index   file from offset       reserved
 *  *   [0x20 0x20]      [... 8 bytes...]   [... 8 bytes...]   [... 8 bytes...]
 *  * <pre>
 *
 *  * Every record format is:
 *  * <pre>
 *   Magic bytes     data length   data
 *   [0x57, 0x8A]    [4 bytes]     [bytes]
 *  *</pre>
 *  *
 */
public class SegmentFile extends AbstractFile {
    private static final IgniteLogger LOG = Loggers.forClass(SegmentFile.class);

    /**
     * Magic bytes for data buffer.
     */
    public static final byte[]  RECORD_MAGIC_BYTES      = new byte[] { (byte) 0x57, (byte) 0x8A };

    public static final int     RECORD_MAGIC_BYTES_SIZE = RECORD_MAGIC_BYTES.length;

    // 4 Bytes for written data length
    private static final int    RECORD_DATA_LENGTH_SIZE = 4;

    public SegmentFile(RaftOptions raftOptions, final String filePath, final int fileSize) {
        super(raftOptions, filePath, fileSize, true);
    }

    /**
     *
     * Write the data and return it's wrote position.
     * @param logIndex the log index
     * @param data     data to write
     * @return the wrote position
     */
    public int appendData(final long logIndex, final byte[] data) {
        this.writeLock.lock();
        try {
            assert (logIndex > getLastLogIndex());

            return doAppend(logIndex, addr -> {
                GridUnsafe.putByte(addr, RECORD_MAGIC_BYTES[0]);
                GridUnsafe.putByte(addr + 1, RECORD_MAGIC_BYTES[1]);

                Bits.putIntLittleEndian(addr + 2, data.length);

                GridUnsafe.copyHeapOffheap(data, GridUnsafe.BYTE_ARR_OFF, addr + 6, data.length);

                return getWriteBytes(data);
            });
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     *
     * Write the data and return its written position. Based on {@link #appendData(long, byte[])}, but more efficient.
     * @param logIndex the log index
     * @param encoder Log entry encoder
     * @param entry Log entry
     * @param entrySize Pre-calculated serialized entry size
     * @return the wrote position
     */
    public int appendData(final long logIndex, V1Encoder encoder, LogEntry entry, int entrySize) {
        this.writeLock.lock();
        try {
            assert (logIndex > getLastLogIndex());

            return doAppend(logIndex, addr -> {
                GridUnsafe.putByte(addr, RECORD_MAGIC_BYTES[0]);
                GridUnsafe.putByte(addr + 1, RECORD_MAGIC_BYTES[1]);

                Bits.putIntLittleEndian(addr + 2, entrySize);

                encoder.append(addr + 6, entry);

                return entrySize + 6;
            });
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Read data from the position.
     *
     * @param logIndex the log index
     * @param pos      the position to read
     * @return read data
     */
    public byte[] lookupData(final long logIndex, final int pos) {
        assert (pos >= this.header.getHeaderSize());
        mapInIfNecessary();
        this.readLock.lock();
        try {
            if (logIndex < this.header.getFirstLogIndex() || logIndex > this.getLastLogIndex()) {
                LOG.warn(
                    "Try to read data from segment file {} out of range, logIndex={}, readPos={}, firstLogIndex={}, lastLogIndex={}.",
                    getFilePath(), logIndex, pos, this.header.getFirstLogIndex(), getLastLogIndex());
                return null;
            }
            // Original jraft code did the comparison with flushed position. In didn't work in cases where leader would write log entry
            // locally, wouldn't flush it, and then will try replicating it. I don't know whether it's correct, but this is how it works.
            if (pos > getWrotePosition()) {
                LOG.warn(
                    "Try to read data from segment file {} out of written position, logIndex={}, readPos={}, wrotePos={}, flushPos={}.",
                    getFilePath(), logIndex, pos, getWrotePosition(), getFlushedPosition());
                return null;
            }
            return lookupData(pos);
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * Read data from the position
     * @param pos      the position to read
     * @return read data
     */
    public byte[] lookupData(final int pos) {
        assert (pos >= this.header.getHeaderSize());
        mapInIfNecessary();
        this.readLock.lock();
        try {
            final ByteBuffer readBuffer = sliceByteBuffer();
            readBuffer.position(pos);
            if (readBuffer.remaining() < RECORD_MAGIC_BYTES_SIZE) {
                return null;
            }
            final byte[] magic = new byte[RECORD_MAGIC_BYTES_SIZE];
            readBuffer.get(magic);
            if (!Arrays.equals(magic, RECORD_MAGIC_BYTES)) {
                return null;
            }
            final int dataLen = readBuffer.getInt();
            if (dataLen <= 0) {
                return null;
            }
            final byte[] data = new byte[dataLen];
            readBuffer.get(data);
            return data;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public CheckDataResult checkData(final ByteBuffer buffer) {
        assert buffer.order() == LOGIT_BYTE_ORDER;

        if (buffer.remaining() < RECORD_MAGIC_BYTES_SIZE) {
            return CheckDataResult.CHECK_FAIL;
        }
        // Check magic
        final byte[] magic = new byte[RECORD_MAGIC_BYTES_SIZE];
        buffer.get(magic);
        if (!Arrays.equals(magic, RECORD_MAGIC_BYTES)) {
            return CheckDataResult.FILE_END;
        }
        // Check len
        if (buffer.remaining() < RECORD_DATA_LENGTH_SIZE) {
            return CheckDataResult.CHECK_FAIL;
        }
        final int dataLen = buffer.getInt();
        if (buffer.remaining() < dataLen) {
            return CheckDataResult.CHECK_FAIL;
        }
        return new CheckDataResult(RECORD_MAGIC_BYTES_SIZE + RECORD_DATA_LENGTH_SIZE + dataLen);
    }

    @Override
    public void onRecoverDone(final int recoverPosition) {
        // Since the logs index in the segmentFile are discontinuous, we should set LastLogIndex by reading and deSerializing last entry log
        final ByteBuffer buffer = sliceByteBuffer();
        buffer.position(recoverPosition);
        final byte[] data = lookupData(recoverPosition);
        if (data != null) {
            final LogEntry lastEntry = DefaultLogEntryCodecFactory.getInstance().decoder().decode(data);
            if (lastEntry != null) {
                setLastLogIndex(lastEntry.getId().getIndex());
            }
        }
    }

    @Override
    public int truncate(final long logIndex, final int pos) {
        this.writeLock.lock();
        try {
            if (logIndex < this.header.getFirstLogIndex() || logIndex > this.header.getLastLogIndex()) {
                return 0;
            }
            if (pos < 0) {
                return getWrotePosition();
            }
            updateAllPosition(pos);
            clear(getWrotePosition());
            this.header.setLastLogIndex(logIndex - 1);
            return pos;
        } finally {
            this.writeLock.unlock();
        }
    }

    public static int getWriteBytes(final byte[] data) {
        return getWriteBytes(data.length);
    }

    public static int getWriteBytes(int dataSize) {
        return RECORD_MAGIC_BYTES_SIZE + RECORD_DATA_LENGTH_SIZE + dataSize;
    }
}
