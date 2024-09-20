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

package org.apache.ignite.raft.jraft.storage.logit.storage.file;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.util.function.LongToIntFunction;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.logit.LibC;
import org.apache.ignite.raft.jraft.storage.logit.util.concurrent.ReferenceResource;
import org.apache.ignite.raft.jraft.util.Platform;
import org.apache.ignite.raft.jraft.util.Utils;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;

import sun.nio.ch.DirectBuffer;

/**
 * File parent class that wrappers uniform functions such as mmap(), flush() etc..
 */
public abstract class AbstractFile extends ReferenceResource {
    private static final IgniteLogger LOG = Loggers.forClass(AbstractFile.class);

    /** Byte order that's used to encode data in log. */
    public static final ByteOrder LOGIT_BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    protected static final int    BLANK_HOLE_SIZE = 64;

    protected static final byte   FILE_END_BYTE   = 'x';

    private final RaftOptions raftOptions;

    protected String              filePath;

    // The size of this file
    protected int                 fileSize;

    protected File                file;

    protected FileHeader          header;

    protected MappedByteBuffer    mappedByteBuffer;

    // Current write position
    protected volatile int wrotePosition = 0;

    // Current flush position
    protected volatile int flushedPosition = 0;

    protected final ReadWriteLock readWriteLock   = new ReentrantReadWriteLock();
    protected final Lock          readLock        = this.readWriteLock.readLock();
    protected final Lock          writeLock       = this.readWriteLock.writeLock();
    private volatile boolean      isMapped        = false;
    private final ReentrantLock   mapLock         = new ReentrantLock();

    public AbstractFile(RaftOptions raftOptions, final String filePath, final int fileSize, final boolean isMapped) {
        this.raftOptions = raftOptions;
        initAndMap(filePath, fileSize, isMapped);
        this.header = new FileHeader();
    }

    public void initAndMap(final String filePath, final int fileSize, final boolean isMapped) {
        this.filePath = filePath;
        this.fileSize = fileSize;
        this.file = new File(filePath);
        this.mapLock.lock();
        try {
            if (!this.file.exists()) {
                if (!this.file.createNewFile()) {
                    throw new RuntimeException("Failed to create new file");
                }
            }
            if (isMapped) {
                this.map(MapMode.READ_WRITE);
            }
        } catch (final Throwable t) {
            LOG.error("Error happen when create file:{}", filePath, t);
        } finally {
            this.mapLock.unlock();
        }
    }

    public void mapInIfNecessary() {
        if (!isMapped()) {
            this.map(MapMode.READ_ONLY);
        }
    }

    public void map(final MapMode mapMode) {
        this.mapLock.lock();
        try {
            if (!isMapped()) {
                try (final RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "rw");
                        final FileChannel fileChannel = randomAccessFile.getChannel()) {
                    this.mappedByteBuffer = fileChannel.map(mapMode, 0, this.fileSize);
                    mappedByteBuffer.order(LOGIT_BYTE_ORDER);
                    this.isMapped = true;
                }
            }
        } catch (final Throwable t) {
            LOG.error("map file {} failed , {}", getFilePath(), t);
            throw new RuntimeException(t);
        } finally {
            this.mapLock.unlock();
        }
    }

    public void unmmap() {
        if (isMapped()) {
            this.mapLock.lock();
            try {
                if (isMapped()) {
                    this.mappedByteBuffer.force();
                    this.flushedPosition = getWrotePosition();
                    if (this.mappedByteBuffer != null) {
                        if (Platform.isLinux()) {
                            hintUnload();
                        }
                        Utils.unmap(this.mappedByteBuffer);
                    }
                    this.isMapped = false;
                }
            } catch (final Throwable t) {
                LOG.error("error unmap file {} , {}", getFilePath(), t);
                throw new RuntimeException(t);
            } finally {
                this.mapLock.unlock();
            }
        }
    }

    public boolean isMapped() {
        return this.isMapped;
    }

    public static class RecoverResult {
        // Is recover success
        private final boolean recoverSuccess;
        // Is recover total data or encounter an error when recover
        private final boolean recoverTotal;
        // Last recover offset
        private final int     lastOffset;

        public RecoverResult(final boolean recoverSuccess, final boolean recoverTotal, final int lastOffset) {
            this.recoverSuccess = recoverSuccess;
            this.recoverTotal = recoverTotal;
            this.lastOffset = lastOffset;
        }

        public static RecoverResult newInstance(final boolean isSuccess, final boolean isRecoverTotal,
                                                final int recoverOffset) {
            return new RecoverResult(isSuccess, isRecoverTotal, recoverOffset);
        }

        public int getLastOffset() {
            return lastOffset;
        }

        public boolean recoverSuccess() {
            return recoverSuccess;
        }

        public boolean recoverTotal() {
            return recoverTotal;
        }
    }

    /**
     * Recover logic
     * @return the last recover position
     */
    public RecoverResult recover() {
        if (!loadHeader()) {
            return RecoverResult.newInstance(false, false, -1);
        }
        final ByteBuffer byteBuffer = sliceByteBuffer();
        int recoverPosition = this.header.getHeaderSize();
        int recoverCnt = 0;
        int lastLogPosition = recoverPosition;
        boolean isFileEnd = false;
        final long start = Utils.monotonicMs();
        while (true) {
            byteBuffer.position(recoverPosition);
            final CheckDataResult checkResult = checkData(byteBuffer);
            if (checkResult == CheckDataResult.FILE_END) {
                // File end
                isFileEnd = true;
                break;
            } else if (checkResult == CheckDataResult.CHECK_FAIL) {
                // Check fail
                break;
            } else {
                // Check success
                recoverCnt++;
                lastLogPosition = recoverPosition;
                recoverPosition += checkResult.size;
            }
        }
        updateAllPosition(recoverPosition);
        onRecoverDone(lastLogPosition);
        LOG.info("Recover file {} cost {} millis, recoverPosition:{}, recover logs:{}, lastLogIndex:{}", getFilePath(),
            Utils.monotonicMs() - start, recoverPosition, recoverCnt, getLastLogIndex());
        final boolean isRecoverTotal = isFileEnd || (recoverPosition == this.fileSize);
        return RecoverResult.newInstance(true, isRecoverTotal, recoverPosition);
    }

    public static class CheckDataResult {
        public static final CheckDataResult CHECK_FAIL = new CheckDataResult(-1); // If check failed, return -1
        public static final CheckDataResult FILE_END = new CheckDataResult(0); // If come to file end, return 0

        private int size;

        public CheckDataResult(final int pos) {
            this.size = pos;
        }
    }

    /**
     *
     * @return check result
     */
    public abstract CheckDataResult checkData(final ByteBuffer byteBuffer);

    /**
     * Trigger function when recover done
     * @param recoverPosition the recover position
     */
    public abstract void onRecoverDone(final int recoverPosition);

    /**
     * Truncate file entries to logIndex
     * @param logIndex the target logIndex
     * @param pos the position of this entry, this parameter is needed only if this file is a segmentFile
     */
    public abstract int truncate(final long logIndex, final int pos);

    /**
     * Append data to file end
     * @param logIndex logEntry index
     * @param append Data append function
     * @return wrote position
     */
    protected int doAppend(final long logIndex, LongToIntFunction append) {
        this.writeLock.lock();
        try {
            int wrotePos = getWrotePosition();
            // First time append , set firstLogIndex to header
            if (this.header.isBlank()) {
                this.header.setFirstLogIndex(logIndex);
                saveHeader();
                wrotePos = this.header.getHeaderSize();
            }
            // Write data and update header
            final ByteBuffer buffer = sliceByteBuffer();

            long pointer = GridUnsafe.bufferAddress(buffer) + wrotePos;
            int length = append.applyAsInt(pointer);
            setWrotePosition(wrotePos + length);

            this.header.setLastLogIndex(logIndex);
            return wrotePos;
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Flush data to disk
     * @return The current flushed position
     */
    public int flush() {
        if (hold()) {
            final int value = getWrotePosition();
            try {
                if (raftOptions.isSync()) {
                    this.mappedByteBuffer.force();
                }
            } catch (final Throwable e) {
                LOG.error("Error occurred when force data to disk.", e);
                throw new RuntimeException(e);
            }
            setFlushPosition(value);
            release();
        } else {
            LOG.warn("In flush, hold failed, flush offset = {}.", getFlushedPosition());
            setFlushPosition(getWrotePosition());
        }
        return getFlushedPosition();
    }

    public boolean shutdown(final long intervalForcibly, final boolean isDestroy) {
        shutdown(intervalForcibly);
        if (isCleanupOver()) {
            try {
                if (isDestroy) {
                    return IgniteUtils.deleteIfExists(this.file.toPath());
                }
                return true;
            } catch (final Throwable t) {
                LOG.error("Close file channel failed, {}", getFilePath(), t);
                throw new RuntimeException(t);
            }
        }
        return false;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        if (isAvailable()) {
            return false;
        }
        if (isCleanupOver() || !isMapped()) {
            return true;
        }
        unmmap();
        return true;
    }

    /**
     * Load file header
     */
    public boolean loadHeader() {
        final ByteBuffer byteBuffer = sliceByteBuffer();
        byteBuffer.position(0);
        return this.header.decode(byteBuffer);
    }

    /**
     * Save file header
     * Dont need to flush , header will be flushed by flushService
     */
    public void saveHeader() {
        // save header to mappedByteBuffer
        final ByteBuffer header = this.header.encode();
        final ByteBuffer byteBuffer = sliceByteBuffer();
        byteBuffer.position(0);
        byteBuffer.put(header);
    }

    /**
     * Fill empty bytes in this file end when this fill has no sufficient free space to store one message
     */
    public void fillEmptyBytesInFileEnd() {
        this.writeLock.lock();
        try {
            final int wrotePosition = getWrotePosition();
            final ByteBuffer byteBuffer = sliceByteBuffer();
            for (int i = wrotePosition; i < this.fileSize; i++) {
                byteBuffer.put(i, FILE_END_BYTE);
            }
            setWrotePosition(this.fileSize);
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Clear data in [startPos, startPos+64).
     */
    public void clear(final int startPos) {
        this.writeLock.lock();
        try {
            if (startPos < 0 || startPos > this.fileSize) {
                return;
            }
            final int endPos = Math.min(this.fileSize, startPos + BLANK_HOLE_SIZE);
            for (int i = startPos; i < endPos; i++) {
                this.mappedByteBuffer.put(i, (byte) 0);
            }
            this.mappedByteBuffer.force();
            LOG.info("File {} cleared data in [{}, {}].", this.filePath, startPos, endPos);
        } finally {
            this.writeLock.unlock();
        }
    }

    public void put(final ByteBuffer buffer, final int index, final byte[] data) {
        GridUnsafe.copyHeapOffheap(data, GridUnsafe.BYTE_ARR_OFF, GridUnsafe.bufferAddress(buffer) + index, data.length);
    }

    public long getFirstLogIndex() {
        return this.header.getFirstLogIndex();
    }

    public long getLastLogIndex() {
        return this.header.getLastLogIndex();
    }

    public void setLastLogIndex(final long lastLogIndex) {
        this.header.setLastLogIndex(lastLogIndex);
    }

    public void setFileFromOffset(final long fileFromOffset) {
        this.header.setFileFromOffset(fileFromOffset);
    }

    public long getFileFromOffset() {
        return this.header.getFileFromOffset();
    }

    /**
     * Returns true when the segment file contains the log index.
     *
     * @param logIndex the log index
     * @return true if the segment file contains the log index, otherwise return false
     */
    public boolean contains(final long logIndex) {
        this.readLock.lock();
        try {
            return logIndex >= this.header.getFirstLogIndex() && logIndex <= this.getLastLogIndex();
        } finally {
            this.readLock.unlock();
        }
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice().order(LOGIT_BYTE_ORDER);
    }

    public void warmupFile() {
        if (!isMapped()) {
            return;
        }
        // Lock memory
        if (Platform.isLinux()) {
            hintLoad();
        }
    }

    public void hintLoad() {
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);

        long beginTime = Utils.monotonicMs();
        if (Platform.isLinux()) {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            LOG.info("madvise(MADV_WILLNEED) {} {} {} ret = {} time consuming = {}", address, this.filePath,
                this.fileSize, ret, Utils.monotonicMs() - beginTime);
        }
    }

    public void hintUnload() {
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);

        long beginTime = Utils.monotonicMs();
        if (Platform.isLinux()) {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_DONTNEED);
            LOG.info("madvise(MADV_DONTNEED) {} {} {} ret = {} time consuming = {}", address, this.filePath,
                this.fileSize, ret, Utils.monotonicMs() - beginTime);
        }
    }

    public void reset() {
        setWrotePosition(0);
        setFlushPosition(0);
        this.header.setFirstLogIndex(FileHeader.BLANK_OFFSET_INDEX);
        flush();
    }

    public int getWrotePosition() {
        return wrotePosition;
    }

    public void setWrotePosition(final int position) {
        this.wrotePosition = position;
    }

    public int getFlushedPosition() {
        return flushedPosition;
    }

    public void setFlushPosition(final int position) {
        this.flushedPosition = position;
    }

    /**
     * Update  flush / wrote position to pos
     * @param pos target pos
     */
    public void updateAllPosition(final int pos) {
        setWrotePosition(pos);
        setFlushPosition(pos);
    }

    public String getFilePath() {
        return filePath;
    }

    public boolean reachesFileEndBy(final int waitToWroteSize) {
        return getWrotePosition() + waitToWroteSize > getFileSize();
    }

    public int getFileSize() {
        return fileSize;
    }

    public boolean isBlank() {
        return this.header.isBlank();
    }
}
