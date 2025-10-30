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

import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayload.HASH_SIZE_BYTES;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayload.TRUNCATE_SUFFIX_RECORD_SIZE;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.raft.storage.segstore.SegmentFile.WriteBuffer;
import org.apache.ignite.internal.raft.util.VarlenEncoder;
import org.apache.ignite.internal.util.FastCrc;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryDecoder;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryEncoder;
import org.jetbrains.annotations.Nullable;

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
 * <p>Every appended entry is converted into its serialized form (a.k.a. "payload"), defined by a {@link LogEntryEncoder},
 * and stored in a segment file.
 *
 * <p>Binary representation of each entry is as follows:
 * <pre>
 * +-------------------------+--------------------------+--------------------+-------------------+---------+----------------+
 * | Raft Group ID (8 bytes) | Payload Length (4 bytes) | Index (1-10 bytes) | Term (1-10 bytes) | Payload | Hash (4 bytes) |
 * +-------------------------+--------------------------+--------------------+-------------------+---------+----------------+
 * </pre>
 *
 * <p>Log Entry Index and Term are stored as variable-length integers (varints), hence the non-fixed size in bytes. They are treated as
 * a part of the payload, so payload length includes their size as well.
 *
 * <p>In addition to regular Raft log entries, payload can also represent a special type of entry which are written when Raft suffix
 * is truncated. Such entries are identified by having a payload length of 0, followed by 8 bytes of the last log index kept after the
 * truncation.
 *
 * <p>When a rollover happens and the segment file being replaced has at least 8 bytes left, a special {@link #SWITCH_SEGMENT_RECORD} is
 * written at the end of the file. If there are less than 8 bytes left, no switch records are written.
 */
class SegmentFileManager implements ManuallyCloseable {
    private static final int ROLLOVER_WAIT_TIMEOUT_MS = 30_000;

    private static final int MAGIC_NUMBER = 0x56E0B526;

    private static final int FORMAT_VERSION = 1;

    private static final String SEGMENT_FILE_NAME_FORMAT = "segment-%010d-%010d.bin";

    private static final Pattern SEGMENT_FILE_NAME_PATTERN = Pattern.compile("segment-(?<ordinal>\\d{10})-(?<generation>\\d{10})\\.bin");

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

    private final Path segmentFilesDir;

    /** Configured size of a segment file. */
    private final long fileSize;

    /** Number of stripes used by the index memtable. Should be equal to the number of stripes in the Raft server's Disruptor. */
    private final int stripes;

    /**
     * Current segment file. While a rollover is in progress, its content will be {@link SegmentFileWithMemtable#readOnly() read-only}.
     */
    private final AtomicReference<SegmentFileWithMemtable> currentSegmentFile = new AtomicReference<>();

    private final RaftLogCheckpointer checkpointer;

    private final IndexFileManager indexFileManager;

    /** Lock used to block threads while a rollover is in progress. */
    private final Object rolloverLock = new Object();

    /**
     * Current segment file ordinal (used to generate segment file names).
     */
    private volatile int curSegmentFileOrdinal;

    /**
     * Flag indicating whether the file manager has been stopped.
     *
     * <p>Must always be accessed under the {@link #rolloverLock}.
     */
    private boolean isStopped;

    SegmentFileManager(String nodeName, Path baseDir, long fileSize, int stripes, FailureProcessor failureProcessor) throws IOException {
        if (fileSize <= HEADER_RECORD.length) {
            throw new IllegalArgumentException("File size must be greater than the header size: " + fileSize);
        }

        this.segmentFilesDir = baseDir.resolve("segments");

        Files.createDirectories(segmentFilesDir);

        this.fileSize = fileSize;
        this.stripes = stripes;

        indexFileManager = new IndexFileManager(baseDir);
        checkpointer = new RaftLogCheckpointer(nodeName, indexFileManager, failureProcessor);
    }

    void start() throws IOException {
        Path lastSegmentFilePath = null;

        try (Stream<Path> segmentFiles = Files.list(segmentFilesDir)) {
            Iterator<Path> it = segmentFiles.sorted().iterator();

            while (it.hasNext()) {
                Path segmentFilePath = it.next();

                if (!it.hasNext()) {
                    // Last segment file is treated differently.
                    lastSegmentFilePath = segmentFilePath;
                } else {
                    // Create missing index files.
                    int segmentFileOrdinal = segmentFileOrdinal(segmentFilePath);

                    if (!indexFileManager.indexFileExists(segmentFileOrdinal)) {
                        SegmentFile segmentFile = SegmentFile.openExisting(segmentFilePath);

                        WriteModeIndexMemTable memTable = recoverMemtable(segmentFile, segmentFilePath);

                        indexFileManager.saveIndexMemtable(memTable.transitionToReadMode(), segmentFileOrdinal);
                    }
                }
            }
        }

        if (lastSegmentFilePath == null) {
            currentSegmentFile.set(allocateNewSegmentFile(0));
        } else {
            curSegmentFileOrdinal = segmentFileOrdinal(lastSegmentFilePath);

            currentSegmentFile.set(recoverLatestSegmentFile(lastSegmentFilePath));
        }

        // Index File Manager must be started strictly before the checkpointer.
        indexFileManager.start();

        checkpointer.start();
    }

    Path segmentFilesDir() {
        return segmentFilesDir;
    }

    Path indexFilesDir() {
        return indexFileManager.indexFilesDir();
    }

    private SegmentFileWithMemtable allocateNewSegmentFile(int fileOrdinal) throws IOException {
        Path path = segmentFilesDir.resolve(segmentFileName(fileOrdinal, 0));

        SegmentFile segmentFile = SegmentFile.createNew(path, fileSize);

        writeHeader(segmentFile);

        return new SegmentFileWithMemtable(segmentFile, new IndexMemTable(stripes), false);
    }

    private SegmentFileWithMemtable recoverLatestSegmentFile(Path segmentFilePath) throws IOException {
        SegmentFile segmentFile = SegmentFile.openExisting(segmentFilePath);

        return new SegmentFileWithMemtable(segmentFile, recoverLatestMemtable(segmentFile, segmentFilePath), false);
    }

    private static String segmentFileName(int fileOrdinal, int generation) {
        return String.format(SEGMENT_FILE_NAME_FORMAT, fileOrdinal, generation);
    }

    private static SegmentFileWithMemtable convertToReadOnly(SegmentFileWithMemtable segmentFile) {
        return new SegmentFileWithMemtable(segmentFile.segmentFile(), segmentFile.memtable(), true);
    }

    void appendEntry(long groupId, LogEntry entry, LogEntryEncoder encoder) throws IOException {
        int segmentEntrySize = SegmentPayload.size(entry, encoder);

        if (segmentEntrySize > maxPossibleEntrySize()) {
            throw new IllegalArgumentException(String.format(
                    "Segment entry is too big (%d bytes), maximum allowed segment entry size: %d bytes.",
                    segmentEntrySize, maxPossibleEntrySize()
            ));
        }

        try (WriteBufferWithMemtable writeBufferWithMemtable = reserveBytesWithRollover(segmentEntrySize)) {
            ByteBuffer segmentBuffer = writeBufferWithMemtable.buffer();

            int segmentOffset = segmentBuffer.position();

            SegmentPayload.writeTo(segmentBuffer, groupId, segmentEntrySize, entry, encoder);

            // Append to memtable before write buffer is released to avoid races with checkpoint on rollover.
            writeBufferWithMemtable.memtable.appendSegmentFileOffset(groupId, entry.getId().getIndex(), segmentOffset);
        }
    }

    @Nullable LogEntry getEntry(long groupId, long logIndex, LogEntryDecoder decoder) throws IOException {
        ByteBuffer entryBuffer = getEntry(groupId, logIndex);

        return entryBuffer == null ? null : SegmentPayload.readFrom(entryBuffer, decoder);
    }

    private @Nullable ByteBuffer getEntry(long groupId, long logIndex) throws IOException {
        // First, read from the current segment file.
        SegmentFileWithMemtable currentSegmentFile = this.currentSegmentFile.get();

        SegmentInfo segmentInfo = currentSegmentFile.memtable().segmentInfo(groupId);

        if (segmentInfo != null) {
            if (logIndex >= segmentInfo.lastLogIndexExclusive()) {
                return null;
            }

            int segmentPayloadOffset = segmentInfo.getOffset(logIndex);

            if (segmentPayloadOffset != 0) {
                return currentSegmentFile.segmentFile().buffer().position(segmentPayloadOffset);
            }
        }

        ByteBuffer bufferFromCheckpointQueue = checkpointer.findSegmentPayloadInQueue(groupId, logIndex);

        if (bufferFromCheckpointQueue != null) {
            return bufferFromCheckpointQueue;
        }

        return readFromOtherSegmentFiles(groupId, logIndex);
    }

    void truncateSuffix(long groupId, long lastLogIndexKept) throws IOException {
        try (WriteBufferWithMemtable writeBufferWithMemtable = reserveBytesWithRollover(TRUNCATE_SUFFIX_RECORD_SIZE)) {
            ByteBuffer segmentBuffer = writeBufferWithMemtable.buffer();

            SegmentPayload.writeTruncateSuffixRecordTo(segmentBuffer, groupId, lastLogIndexKept);

            // Modify the memtable before write buffer is released to avoid races with checkpoint on rollover.
            writeBufferWithMemtable.memtable.truncateSuffix(groupId, lastLogIndexKept);
        }
    }

    private WriteBufferWithMemtable reserveBytesWithRollover(int size) throws IOException {
        while (true) {
            SegmentFileWithMemtable segmentFileWithMemtable = currentSegmentFile();

            WriteBuffer writeBuffer = segmentFileWithMemtable.segmentFile().reserve(size);

            if (writeBuffer != null) {
                return new WriteBufferWithMemtable(writeBuffer, segmentFileWithMemtable.memtable());
            }

            // Segment file does not have enough space. Try to switch to a new one and retry the write attempt.
            initiateRollover(segmentFileWithMemtable);
        }
    }

    /**
     * Returns the lowest log index for the given group present in the storage or {@code -1} if no such index exists.
     */
    long firstLogIndexInclusive(long groupId) {
        long logIndexFromMemtable = firstLogIndexFromMemtable(groupId);

        long logIndexFromCheckpointQueue = checkpointer.firstLogIndexInclusive(groupId);

        long logIndexFromIndexFiles = indexFileManager.firstLogIndexInclusive(groupId);

        if (logIndexFromIndexFiles >= 0) {
            return logIndexFromIndexFiles;
        }

        if (logIndexFromCheckpointQueue >= 0) {
            return logIndexFromCheckpointQueue;
        }

        return logIndexFromMemtable;
    }

    private long firstLogIndexFromMemtable(long groupId) {
        SegmentFileWithMemtable currentSegmentFile = this.currentSegmentFile.get();

        SegmentInfo segmentInfo = currentSegmentFile.memtable().segmentInfo(groupId);

        if (segmentInfo == null || segmentInfo.size() == 0) {
            return -1;
        }

        return segmentInfo.firstLogIndexInclusive();
    }

    /**
     * Returns the highest possible exclusive log index for the given group or {@code -1} if no such index exists.
     *
     * <p>The highest log index currently present in the storage can be computed as {@code lastLogIndexExclusive - 1}.
     */
    long lastLogIndexExclusive(long groupId) {
        long logIndexFromMemtable = lastLogIndexFromMemtable(groupId);

        if (logIndexFromMemtable >= 0) {
            return logIndexFromMemtable;
        }

        long logIndexFromCheckpointQueue = checkpointer.lastLogIndexExclusive(groupId);

        if (logIndexFromCheckpointQueue >= 0) {
            return logIndexFromCheckpointQueue;
        }

        return indexFileManager.lastLogIndexExclusive(groupId);
    }

    private long lastLogIndexFromMemtable(long groupId) {
        SegmentFileWithMemtable currentSegmentFile = this.currentSegmentFile.get();

        SegmentInfo segmentInfo = currentSegmentFile.memtable().segmentInfo(groupId);

        return segmentInfo == null ? -1 : segmentInfo.lastLogIndexExclusive();
    }

    /**
     * Returns the current segment file possibly waiting for an ongoing rollover to complete.
     */
    private SegmentFileWithMemtable currentSegmentFile() {
        SegmentFileWithMemtable segmentFile = currentSegmentFile.get();

        if (!segmentFile.readOnly()) {
            return segmentFile;
        }

        // If the current segment file is read-only, then a rollover is in progress and we need to wait for it to complete.
        try {
            synchronized (rolloverLock) {
                while (true) {
                    if (isStopped) {
                        throw new IgniteInternalException(NODE_STOPPING_ERR);
                    }

                    segmentFile = currentSegmentFile.get();

                    if (!segmentFile.readOnly()) {
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

    private void initiateRollover(SegmentFileWithMemtable observedSegmentFile) throws IOException {
        if (!currentSegmentFile.compareAndSet(observedSegmentFile, convertToReadOnly(observedSegmentFile))) {
            // Other thread initiated the rollover or the file manager has been stopped. In both cases we do nothing and will handle this
            // situation in a consecutive "currentSegmentFile" call by either waiting for the rollover to complete or throwing an exception.
            return;
        }

        checkpointer.onRollover(
                observedSegmentFile.segmentFile(),
                observedSegmentFile.memtable().transitionToReadMode()
        );

        synchronized (rolloverLock) {
            if (isStopped) {
                throw new IgniteInternalException(NODE_STOPPING_ERR);
            }

            currentSegmentFile.set(allocateNewSegmentFile(++curSegmentFileOrdinal));

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

            SegmentFileWithMemtable segmentFile = currentSegmentFile.get();

            // This should usually not happen but can happen on an abrupt node stop.
            if (segmentFile != null) {
                segmentFile.segmentFile().close();
            }

            rolloverLock.notifyAll();
        }

        checkpointer.stop();
    }

    private static void writeHeader(SegmentFile segmentFile) {
        try (WriteBuffer writeBuffer = segmentFile.reserve(HEADER_RECORD.length)) {
            // This is always called when a segment file is being created, so we expect to have enough space.
            assert writeBuffer != null;

            writeBuffer.buffer().put(HEADER_RECORD);
        }
    }

    private long maxPossibleEntrySize() {
        return fileSize - HEADER_RECORD.length;
    }

    private @Nullable ByteBuffer readFromOtherSegmentFiles(long groupId, long logIndex) throws IOException {
        SegmentFilePointer segmentFilePointer = indexFileManager.getSegmentFilePointer(groupId, logIndex);

        if (segmentFilePointer == null) {
            return null;
        }

        Path path = segmentFilesDir.resolve(segmentFileName(segmentFilePointer.fileOrdinal(), 0));

        // TODO: Add a cache for recently accessed segment files, see https://issues.apache.org/jira/browse/IGNITE-26622.
        SegmentFile segmentFile = SegmentFile.openExisting(path);

        return segmentFile.buffer().position(segmentFilePointer.payloadOffset());
    }

    private WriteModeIndexMemTable recoverMemtable(SegmentFile segmentFile, Path segmentFilePath) {
        ByteBuffer buffer = segmentFile.buffer();

        validateSegmentFileHeader(buffer, segmentFilePath);

        var memtable = new IndexMemTable(stripes);

        while (buffer.remaining() > SWITCH_SEGMENT_RECORD.length) {
            int segmentFilePayloadOffset = buffer.position();

            long groupId = buffer.getLong();

            int payloadLength = buffer.getInt();

            int endOfRecordPosition = buffer.position() + payloadLength + HASH_SIZE_BYTES;

            long index = VarlenEncoder.readLong(buffer);

            memtable.appendSegmentFileOffset(groupId, index, segmentFilePayloadOffset);

            buffer.position(endOfRecordPosition);
        }

        return memtable;
    }

    /**
     * Creates an index memtable from the given segment file. Unlike {@link #recoverMemtable} which is expected to only be called on
     * "complete" segment files (i.e. those that has experienced a rollover), this method is expected to be called on the most recent,
     * possibly incomplete segment file.
     */
    private WriteModeIndexMemTable recoverLatestMemtable(SegmentFile segmentFile, Path segmentFilePath) {
        ByteBuffer buffer = segmentFile.buffer();

        validateSegmentFileHeader(buffer, segmentFilePath);

        var memtable = new IndexMemTable(stripes);

        while (buffer.remaining() > SWITCH_SEGMENT_RECORD.length) {
            int segmentFilePayloadOffset = buffer.position();

            long groupId = buffer.getLong();

            int payloadLength = buffer.getInt();

            int crcPosition = buffer.position() + payloadLength;

            long index = VarlenEncoder.readLong(buffer);

            int crc = buffer.getInt(crcPosition);

            buffer.position(segmentFilePayloadOffset);

            int expectedCrc = FastCrc.calcCrc(buffer, crcPosition - segmentFilePayloadOffset);

            // CRC violation signals the end of meaningful data in the segment file.
            if (crc != expectedCrc) {
                break;
            }

            memtable.appendSegmentFileOffset(groupId, index, segmentFilePayloadOffset);
        }

        return memtable;
    }

    private static void validateSegmentFileHeader(ByteBuffer buffer, Path segmentFilePath) {
        int magicNumber = buffer.getInt();

        if (magicNumber != MAGIC_NUMBER) {
            throw new IllegalStateException(String.format("Invalid magic number in segment file %s: %d.", segmentFilePath, magicNumber));
        }

        int formatVersion = buffer.getInt();

        if (formatVersion > FORMAT_VERSION) {
            throw new IllegalStateException(String.format(
                    "Unsupported format version in segment file %s: %d.", segmentFilePath, formatVersion
            ));
        }
    }

    private static int segmentFileOrdinal(Path segmentFile) {
        String fileName = segmentFile.getFileName().toString();

        Matcher matcher = SEGMENT_FILE_NAME_PATTERN.matcher(fileName);

        if (!matcher.matches()) {
            throw new IllegalArgumentException(String.format("Invalid segment file name format: %s.", segmentFile));
        }

        return Integer.parseInt(matcher.group("ordinal"));
    }

    private static class WriteBufferWithMemtable implements AutoCloseable {
        final WriteBuffer writeBuffer;

        final WriteModeIndexMemTable memtable;

        WriteBufferWithMemtable(WriteBuffer writeBuffer, WriteModeIndexMemTable memtable) {
            this.writeBuffer = writeBuffer;
            this.memtable = memtable;
        }

        ByteBuffer buffer() {
            return writeBuffer.buffer();
        }

        @Override
        public void close() {
            writeBuffer.close();
        }
    }
}
