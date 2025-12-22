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

package org.apache.ignite.internal.raft.storage.segstore;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.jetbrains.annotations.Nullable;

/**
 * This class represents an append-only memory-mapped segment file.
 *
 * <p>This implementation is thread-safe in terms of concurrent writes.
 */
class SegmentFile implements ManuallyCloseable {
    /**
     * Byte order of the buffers used by {@link WriteBuffer#buffer}.
     */
    static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    /**
     * Special value that, when stored in {@link #bufferPosition}, means that the file is closed.
     */
    private static final int CLOSED_POS_MARKER = -1;

    private static final MappedByteBufferSyncer SYNCER = MappedByteBufferSyncer.createSyncer();

    private final MappedByteBuffer buffer;

    /** Flag indicating if an fsync call should follow every write to the buffer. */
    private final boolean isSync;

    /** Position of the first non-reserved byte in the buffer. */
    private final AtomicInteger bufferPosition = new AtomicInteger();

    /** Number of concurrent writers to the buffer. */
    private final AtomicInteger numWriters = new AtomicInteger();

    /** Position in the buffer <b>up to which</b> some data has been written. */
    private volatile int lastWritePosition;

    /** Position in the buffer <b>up to which</b> all written bytes have been synced. */
    private volatile int syncPosition;

    /** Lock used to atomically execute fsync. */
    private final Object syncLock = new Object();

    private SegmentFile(RandomAccessFile file, boolean isSync) throws IOException {
        //noinspection ChannelOpenedButNotSafelyClosed
        buffer = file.getChannel().map(MapMode.READ_WRITE, 0, file.length());

        this.isSync = isSync;
    }

    static SegmentFile createNew(Path path, long fileSize, boolean isSync) throws IOException {
        if (fileSize < 0) {
            throw new IllegalArgumentException("File size is negative: " + fileSize);
        }

        // FIXME: remove this limitation and replace the check with MAX_UNSIGNED_INT,
        //  see https://issues.apache.org/jira/browse/IGNITE-26406
        if (fileSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("File size is too big: " + fileSize);
        }

        try (var file = new RandomAccessFile(path.toFile(), "rw")) {
            file.setLength(fileSize);

            return new SegmentFile(file, isSync);
        }
    }

    static SegmentFile openExisting(Path path, boolean isSync) throws IOException {
        if (!Files.exists(path)) {
            throw new IllegalArgumentException("File does not exist: " + path);
        }

        try (var file = new RandomAccessFile(path.toFile(), "rw")) {
            return new SegmentFile(file, isSync);
        }
    }

    ByteBuffer buffer() {
        return buffer.duplicate().order(BYTE_ORDER);
    }

    class WriteBuffer implements AutoCloseable {
        private final ByteBuffer slice;

        private final int pos;

        WriteBuffer(ByteBuffer slice) {
            this.slice = slice;
            this.pos = slice.position();
        }

        ByteBuffer buffer() {
            return slice;
        }

        @Override
        public void close() {
            if (isSync) {
                // Wait for all previous writes to complete.
                while (lastWritePosition != pos) {
                    Thread.onSpinWait();
                }

                lastWritePosition = slice.limit();

                sync(slice.limit());
            }

            numWriters.decrementAndGet();
        }
    }

    /**
     * Closes the file with a rollover intention. This means that before the file is closed and if the file contains enough space, then the
     * given bytes will be appended to the end of the file.
     *
     * <p>It is guaranteed that the given bytes will be written last even in presence of concurrent writers.
     */
    void closeForRollover(byte[] bytesToWrite) {
        close(bytesToWrite);
    }

    @Override
    public void close() {
        close(null);
    }

    private void close(byte @Nullable [] bytesToWrite) {
        int pos = bufferPosition.getAndSet(CLOSED_POS_MARKER);

        if (pos == CLOSED_POS_MARKER) {
            return;
        }

        while (numWriters.get() > 0) {
            Thread.onSpinWait();
        }

        if (bytesToWrite != null && pos + bytesToWrite.length <= buffer.limit()) {
            slice(pos, bytesToWrite.length).put(bytesToWrite);

            lastWritePosition = pos + bytesToWrite.length;
        } else {
            lastWritePosition = pos;
        }
    }

    /**
     * Reserves the given amount of bytes at the end of this file.
     *
     * <p>If the bytes have been reserved successfully, then a {@link WriteBuffer} instance is returned, containing a slice of the mapped
     * byte buffer. If there's not enough space to reserve the given amount of bytes, then {@code null} is returned.
     */
    @Nullable WriteBuffer reserve(int size) {
        numWriters.incrementAndGet();

        try {
            ByteBuffer slice = reserveBytes(size);

            if (slice == null) {
                // Not enough free space left or the file is closed.
                numWriters.decrementAndGet();

                return null;
            }

            return new WriteBuffer(slice);
        } catch (Throwable e) {
            numWriters.decrementAndGet();

            throw e;
        }
    }

    int lastWritePosition() {
        return lastWritePosition;
    }

    int syncPosition() {
        return syncPosition;
    }

    void sync() {
        sync(lastWritePosition);
    }

    private void sync(int upToPosition) {
        if (syncPosition >= upToPosition) {
            return;
        }

        synchronized (syncLock) {
            int syncPosition = this.syncPosition;

            if (syncPosition >= upToPosition) {
                return;
            }

            //noinspection AccessToStaticFieldLockedOnInstance
            SYNCER.force(buffer, syncPosition, upToPosition - syncPosition);

            this.syncPosition = upToPosition;
        }
    }

    private @Nullable ByteBuffer reserveBytes(int size) {
        while (true) {
            int pos = bufferPosition.get();

            if (pos == CLOSED_POS_MARKER) {
                return null;
            }

            int nextPos = pos + size;

            if (nextPos > buffer.limit()) {
                return null;
            }

            if (bufferPosition.compareAndSet(pos, nextPos)) {
                return slice(pos, size);
            }
        }
    }

    private ByteBuffer slice(int pos, int size) {
        return buffer()
                .position(pos)
                .limit(pos + size);
    }
}
