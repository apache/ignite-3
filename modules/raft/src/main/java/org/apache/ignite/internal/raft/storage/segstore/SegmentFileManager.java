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

import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.raft.storage.segstore.SegmentFile.WriteBuffer;

/**
 * File manager responsible for allocating and maintaining a pointer to the current segment file.
 *
 * <p>When the current segment file becomes full, that is, it does not contain enough bytes left to satisfy a request by one of the writer
 * threads, then a new segment file is allocated and is atomically switched to be the current one. This operation is called rollover.
 *
 * <p>Every segment file has the following structure:
 * <pre>
 * +------------------+---------+-----+---------+
 * | Header (8 bytes) | Payload | ... | Payload |
 * +------------------+---------+-----+---------+
 * </pre>
 *
 * <p>Header structure is the following:
 * <pre>
 * +------------------------+-------------------+
 * | Magic number (4 bytes) | Version (4 bytes) |
 * +------------------------+-------------------+
 * </pre>
 *
 * <p>Payload structure is defined by the outer callers.
 *
 * <p>When a rollover happens and the segment file being replaced has at least 8 bytes left, a special {@link #SWITCH_SEGMENT_RECORD} is
 * written at the end of the file. If there are less than 8 bytes left, no switch records are written.
 */
class SegmentFileManager implements ManuallyCloseable {
    private static final int ROLLOVER_WAIT_TIMEOUT_MS = 30_000;

    private static final int MAGIC_NUMBER = 0xFEEDFACE;

    private static final int FORMAT_VERSION = 1;

    private static final String SEGMENT_FILE_NAME_FORMAT = "segment-%010d-%010d.bin";

    /**
     * Byte sequence that is written at the beginning of every segment file.
     */
    static final byte[] HEADER_RECORD = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES)
            .order(SegmentFile.BYTE_ORDER)
            .putInt(MAGIC_NUMBER)
            .putInt(FORMAT_VERSION)
            .array();

    /**
     * Byte sequence that is written at the end of a segment file when a rollover happens and there is enough space left
     * in the file to accommodate it.
     */
    static final byte[] SWITCH_SEGMENT_RECORD = new byte[8]; // 8 zero bytes.

    private final Path baseDir;

    /** Configured size of a segment file. */
    private final long fileSize;

    /**
     * Current segment file. Can store {@code null} while a rollover is in progress or if the file manager has been stopped.
     */
    private final AtomicReference<SegmentFile> currentSegmentFile = new AtomicReference<>();

    /** Lock used to block threads while a rollover is in progress. */
    private final Object rolloverLock = new Object();

    /**
     * Current segment file index (used to generate segment file names).
     *
     * <p>Must always be accessed under the {@link #rolloverLock}.
     */
    private int curFileIndex;

    /**
     * Flag indicating whether the file manager has been stopped.
     *
     * <p>Must always be accessed under the {@link #rolloverLock}.
     */
    private boolean isStopped;

    SegmentFileManager(Path baseDir, long fileSize) {
        this.baseDir = baseDir;
        this.fileSize = fileSize;
    }

    void start() throws IOException {
        // TODO: implement recovery, see https://issues.apache.org/jira/browse/IGNITE-26283.
        currentSegmentFile.set(allocateNewSegmentFile(0));
    }

    private SegmentFile allocateNewSegmentFile(int fileIndex) throws IOException {
        Path path = baseDir.resolve(segmentFileName(fileIndex, 0));

        var segmentFile = new SegmentFile(path, fileSize, 0);

        writeHeader(segmentFile);

        return segmentFile;
    }

    private static String segmentFileName(int fileIndex, int generation) {
        return String.format(SEGMENT_FILE_NAME_FORMAT, fileIndex, generation);
    }

    WriteBuffer reserve(int size) throws IOException {
        if (size > maxEntrySize()) {
            throw new IllegalArgumentException("Entry size is too big: " + size);
        }

        while (true) {
            SegmentFile segmentFile = currentSegmentFile();

            WriteBuffer writeBuffer = segmentFile.reserve(size);

            if (writeBuffer != null) {
                return writeBuffer;
            }

            // Segment file does not have enough space. Try to switch to a new one and retry the write attempt.
            initiateRollover(segmentFile);
        }
    }

    /**
     * Returns the current segment file possibly waiting for an ongoing rollover to complete.
     */
    private SegmentFile currentSegmentFile() {
        SegmentFile segmentFile = currentSegmentFile.get();

        if (segmentFile != null) {
            return segmentFile;
        }

        // If the current segment file is null, then either the manager is stopped or a rollover is in progress and we need to wait for
        // it to complete.
        try {
            synchronized (rolloverLock) {
                while (true) {
                    if (isStopped) {
                        throw new IgniteInternalException(NODE_STOPPING_ERR);
                    }

                    segmentFile = currentSegmentFile.get();

                    if (segmentFile != null) {
                        return segmentFile;
                    }

                    rolloverLock.wait(ROLLOVER_WAIT_TIMEOUT_MS);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInternalException(INTERNAL_ERR, "Interrupted while waiting for rollover.", e);
        }
    }

    private void initiateRollover(SegmentFile observedSegmentFile) throws IOException {
        if (!currentSegmentFile.compareAndSet(observedSegmentFile, null)) {
            // Other thread initiated the rollover or the file manager has been stopped. In both cases we do nothing and will handle this
            // situation in a consecutive "currentSegmentFile" call by either waiting for the rollover to complete or throwing an exception.
            return;
        }

        // This will block until all ongoing writes have been completed.
        observedSegmentFile.closeForRollover(SWITCH_SEGMENT_RECORD);

        synchronized (rolloverLock) {
            if (isStopped) {
                throw new IgniteInternalException(NODE_STOPPING_ERR);
            }

            SegmentFile newFile = allocateNewSegmentFile(++curFileIndex);

            currentSegmentFile.set(newFile);

            rolloverLock.notifyAll();
        }
    }

    @Override
    public void close() throws Exception {
        synchronized (rolloverLock) {
            if (isStopped) {
                return;
            }

            isStopped = true;

            SegmentFile segmentFile = currentSegmentFile.getAndSet(null);

            // Segment file can be null if a rollover has been initiated but the thread performing the rollover has not entered the
            // synchronized section yet. That thread will then close the segment file on its own and will enter the critical section
            // and check the "isStopped" flag.
            if (segmentFile != null) {
                segmentFile.close();
            }

            rolloverLock.notifyAll();
        }
    }

    private static void writeHeader(SegmentFile segmentFile) {
        try (WriteBuffer writeBuffer = segmentFile.reserve(HEADER_RECORD.length)) {
            // This is always called when a segment file is being created, so we expect to have enough space.
            assert writeBuffer != null;

            writeBuffer.buffer().put(HEADER_RECORD);
        }
    }

    private long maxEntrySize() {
        return fileSize - HEADER_RECORD.length;
    }
}
