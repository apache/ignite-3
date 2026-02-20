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

import static java.lang.Math.toIntExact;
import static org.apache.ignite.internal.raft.configuration.LogStorageConfigurationSchema.UNSPECIFIED_MAX_LOG_ENTRY_SIZE;
import static org.apache.ignite.internal.raft.configuration.LogStorageConfigurationSchema.computeDefaultMaxLogEntrySizeBytes;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentInfo.MISSING_SEGMENT_FILE_OFFSET;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayload.RESET_RECORD_SIZE;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayload.TRUNCATE_PREFIX_RECORD_SIZE;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayload.TRUNCATE_SUFFIX_RECORD_SIZE;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.configuration.LogStorageConfiguration;
import org.apache.ignite.internal.raft.configuration.LogStorageView;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.storage.segstore.EntrySearchResult.SearchOutcome;
import org.apache.ignite.internal.raft.storage.segstore.SegmentFile.WriteBuffer;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryDecoder;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryEncoder;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

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
    private static final IgniteLogger LOG = Loggers.forClass(SegmentFileManager.class);

    private static final int ROLLOVER_WAIT_TIMEOUT_MS = 30_000;

    /**
     * Maximum number of times we try to read data from a segment file returned based the index before giving up and throwing an
     * exception. See {@link #readFromOtherSegmentFiles} for more information.
     */
    private static final int MAX_NUM_INDEX_FILE_READ_RETRIES = 5;

    static final int MAGIC_NUMBER = 0x56E0B526;

    static final int FORMAT_VERSION = 1;

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

    /** Number of stripes used by the index memtable. Should be equal to the number of stripes in the Raft server's Disruptor. */
    private final int stripes;

    /**
     * Current segment file. While a rollover is in progress, its content will be {@link SegmentFileWithMemtable#readOnly() read-only}.
     */
    private final AtomicReference<SegmentFileWithMemtable> currentSegmentFile = new AtomicReference<>();

    private final RaftLogCheckpointer checkpointer;

    private final IndexFileManager indexFileManager;

    private final RaftLogGarbageCollector garbageCollector;

    /** Configured size of a segment file. */
    private final int segmentFileSize;

    /** Configured maximum log entry size. */
    private final int maxLogEntrySize;

    private final boolean isSync;

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

    SegmentFileManager(
            String nodeName,
            Path baseDir,
            int stripes,
            FailureProcessor failureProcessor,
            GroupInfoProvider groupInfoProvider,
            RaftConfiguration raftConfiguration,
            LogStorageConfiguration storageConfiguration
    ) throws IOException {
        this.segmentFilesDir = baseDir.resolve("segments");
        this.stripes = stripes;
        this.isSync = raftConfiguration.fsync().value();

        Files.createDirectories(segmentFilesDir);

        LogStorageView logStorageView = storageConfiguration.value();

        segmentFileSize = toIntExact(logStorageView.segmentFileSizeBytes());

        maxLogEntrySize = maxLogEntrySize(logStorageView);

        indexFileManager = new IndexFileManager(baseDir);

        checkpointer = new RaftLogCheckpointer(
                nodeName,
                indexFileManager,
                failureProcessor,
                logStorageView.maxCheckpointQueueSize()
        );

        garbageCollector = new RaftLogGarbageCollector(segmentFilesDir, indexFileManager, groupInfoProvider);
    }

    void start() throws IOException {
        LOG.info("Starting segment file manager [segmentFilesDir={}, fileSize={}].", segmentFilesDir, segmentFileSize);

        indexFileManager.cleanupLeftoverFiles();
        garbageCollector.cleanupLeftoverFiles();

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
                    FileProperties segmentFileProperties = SegmentFile.fileProperties(segmentFilePath);

                    if (!Files.exists(indexFileManager.indexFilePath(segmentFileProperties))) {
                        LOG.info("Creating missing index file for segment file {}.", segmentFilePath);

                        SegmentFileWithMemtable segmentFileWithMemtable = recoverSegmentFile(segmentFilePath);

                        indexFileManager.recoverIndexFile(segmentFileWithMemtable.memtable().transitionToReadMode(), segmentFileProperties);
                    }
                }
            }
        }

        if (lastSegmentFilePath == null) {
            currentSegmentFile.set(allocateNewSegmentFile(0));
        } else {
            curSegmentFileOrdinal = SegmentFile.fileProperties(lastSegmentFilePath).ordinal();

            currentSegmentFile.set(recoverLatestSegmentFile(lastSegmentFilePath));
        }

        LOG.info("Segment file manager recovery completed. Current segment file: {}.", lastSegmentFilePath);

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

    @TestOnly
    IndexFileManager indexFileManager() {
        return indexFileManager;
    }

    @TestOnly
    RaftLogGarbageCollector garbageCollector() {
        return garbageCollector;
    }

    private SegmentFileWithMemtable allocateNewSegmentFile(int fileOrdinal) throws IOException {
        Path path = segmentFilesDir.resolve(SegmentFile.fileName(new FileProperties(fileOrdinal)));

        SegmentFile segmentFile = SegmentFile.createNew(path, segmentFileSize, isSync);

        writeHeader(segmentFile);

        return new SegmentFileWithMemtable(segmentFile, new StripedMemTable(stripes), false);
    }

    /**
     * Creates an index memtable from the given segment file. Unlike {@link #recoverSegmentFile} which is expected to only be called on
     * "complete" segment files (i.e. those that have experienced a rollover) this method is expected to be called on the most recent,
     * possibly incomplete segment file.
     */
    private SegmentFileWithMemtable recoverLatestSegmentFile(Path segmentFilePath) throws IOException {
        SegmentFile segmentFile = SegmentFile.openExisting(segmentFilePath, isSync);

        var memTable = new StripedMemTable(stripes);

        SegmentPayloadParser.recoverMemtable(segmentFile, memTable, true);

        return new SegmentFileWithMemtable(segmentFile, memTable, false);
    }

    /**
     * Creates an index memtable from the given segment file. This method is expected to be called only on "complete" segment files
     * (i.e. those that have experienced a rollover).
     *
     * <p>This method skips CRC validation, because it is used to identify the end of incomplete segment files (and, by definition, this can
     * never happen during this method's invocation), not to validate storage integrity.
     */
    private SegmentFileWithMemtable recoverSegmentFile(Path segmentFilePath) throws IOException {
        SegmentFile segmentFile = SegmentFile.openExisting(segmentFilePath, isSync);

        var memTable = new SingleThreadMemTable();

        SegmentPayloadParser.recoverMemtable(segmentFile, memTable, false);

        return new SegmentFileWithMemtable(segmentFile, memTable, false);
    }

    private static SegmentFileWithMemtable convertToReadOnly(SegmentFileWithMemtable segmentFile) {
        return new SegmentFileWithMemtable(segmentFile.segmentFile(), segmentFile.memtable(), true);
    }

    void appendEntry(long groupId, LogEntry entry, LogEntryEncoder encoder) throws IOException {
        int segmentEntrySize = SegmentPayload.size(entry, encoder);

        if (segmentEntrySize > maxLogEntrySize) {
            throw new IllegalArgumentException(String.format(
                    "Segment entry is too big (%d bytes), maximum allowed segment entry size: %d bytes.",
                    segmentEntrySize, maxLogEntrySize
            ));
        }

        try (WriteBufferWithMemtable writeBufferWithMemtable = reserveBytesWithRollover(segmentEntrySize)) {
            ByteBuffer segmentBuffer = writeBufferWithMemtable.buffer();

            int segmentOffset = segmentBuffer.position();

            SegmentPayload.writeTo(segmentBuffer, groupId, segmentEntrySize, entry, encoder);

            // Append to memtable before write buffer is released to avoid races with checkpoint on rollover.
            writeBufferWithMemtable.memtable().appendSegmentFileOffset(groupId, entry.getId().getIndex(), segmentOffset);
        }
    }

    @Nullable LogEntry getEntry(long groupId, long logIndex, LogEntryDecoder decoder) throws IOException {
        ByteBuffer entryBuffer = getEntry(groupId, logIndex);

        return entryBuffer == null ? null : SegmentPayload.readFrom(entryBuffer, decoder);
    }

    private @Nullable ByteBuffer getEntry(long groupId, long logIndex) throws IOException {
        EntrySearchResult searchResult = getEntryFromCurrentMemtable(groupId, logIndex);

        if (searchResult.searchOutcome() == SearchOutcome.CONTINUE_SEARCH) {
            searchResult = checkpointer.findSegmentPayloadInQueue(groupId, logIndex);

            if (searchResult.searchOutcome() == SearchOutcome.CONTINUE_SEARCH) {
                searchResult = readFromOtherSegmentFiles(groupId, logIndex, 1);
            }
        }

        switch (searchResult.searchOutcome()) {
            case SUCCESS: return searchResult.entryBuffer();
            case NOT_FOUND: return null;
            default: throw new IllegalStateException("Unexpected search outcome: " + searchResult.searchOutcome());
        }
    }

    private EntrySearchResult getEntryFromCurrentMemtable(long groupId, long logIndex) {
        SegmentFileWithMemtable currentSegmentFile = this.currentSegmentFile.get();

        SegmentInfo segmentInfo = currentSegmentFile.memtable().segmentInfo(groupId);

        if (segmentInfo == null) {
            return EntrySearchResult.continueSearch();
        }

        if (logIndex >= segmentInfo.lastLogIndexExclusive()) {
            return EntrySearchResult.notFound();
        }

        if (logIndex < segmentInfo.firstIndexKept()) {
            // This is a prefix tombstone and it cuts off the log index we search for.
            return EntrySearchResult.notFound();
        }

        int segmentPayloadOffset = segmentInfo.getOffset(logIndex);

        if (segmentPayloadOffset == MISSING_SEGMENT_FILE_OFFSET) {
            return EntrySearchResult.continueSearch();
        }

        ByteBuffer entryBuffer = currentSegmentFile.segmentFile().buffer().position(segmentPayloadOffset);

        return EntrySearchResult.success(entryBuffer);
    }

    void truncateSuffix(long groupId, long lastLogIndexKept) throws IOException {
        try (WriteBufferWithMemtable writeBufferWithMemtable = reserveBytesWithRollover(TRUNCATE_SUFFIX_RECORD_SIZE)) {
            SegmentPayload.writeTruncateSuffixRecordTo(writeBufferWithMemtable.buffer(), groupId, lastLogIndexKept);

            // Modify the memtable before write buffer is released to avoid races with checkpoint on rollover.
            writeBufferWithMemtable.memtable().truncateSuffix(groupId, lastLogIndexKept);
        }
    }

    void truncatePrefix(long groupId, long firstLogIndexKept) throws IOException {
        try (WriteBufferWithMemtable writeBufferWithMemtable = reserveBytesWithRollover(TRUNCATE_PREFIX_RECORD_SIZE)) {
            SegmentPayload.writeTruncatePrefixRecordTo(writeBufferWithMemtable.buffer(), groupId, firstLogIndexKept);

            // Modify the memtable before write buffer is released to avoid races with checkpoint on rollover.
            writeBufferWithMemtable.memtable().truncatePrefix(groupId, firstLogIndexKept);
        }
    }

    void reset(long groupId, long nextLogIndex) throws IOException {
        try (WriteBufferWithMemtable writeBufferWithMemtable = reserveBytesWithRollover(RESET_RECORD_SIZE)) {
            SegmentPayload.writeResetRecordTo(writeBufferWithMemtable.buffer(), groupId, nextLogIndex);

            // Modify the memtable before write buffer is released to avoid races with checkpoint on rollover.
            writeBufferWithMemtable.memtable().reset(groupId, nextLogIndex);
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
     *
     * <p>This method is expected to be called without any ongoing load (e.g. on recovery), because it only reflects the state of the
     * storage, not taking pending in-memory state into account.
     */
    long firstLogIndexInclusiveOnRecovery(long groupId) {
        SegmentFileWithMemtable currentSegmentFile = this.currentSegmentFile.get();

        SegmentInfo segmentInfo = currentSegmentFile.memtable().segmentInfo(groupId);

        // We need to consult with the latest memtable in case it contains a prefix tombstone.
        if (segmentInfo != null && segmentInfo.firstIndexKept() != -1) {
            return segmentInfo.firstIndexKept();
        }

        long firstLogIndexFromIndexStorage = indexFileManager.firstLogIndexInclusive(groupId);

        if (firstLogIndexFromIndexStorage != -1) {
            return firstLogIndexFromIndexStorage;
        }

        return segmentInfo == null ? -1 : segmentInfo.firstLogIndexInclusive();
    }

    /**
     * Returns the highest possible exclusive log index for the given group or {@code -1} if no such index exists.
     *
     * <p>The highest log index currently present in the storage can be computed as {@code lastLogIndexExclusive - 1}.
     *
     * <p>This method is expected to be called without any ongoing load (e.g. on recovery), because it only reflects the state of the
     * storage, not taking pending in-memory state into account.
     */
    long lastLogIndexExclusiveOnRecovery(long groupId) {
        SegmentFileWithMemtable currentSegmentFile = this.currentSegmentFile.get();

        SegmentInfo segmentInfo = currentSegmentFile.memtable().segmentInfo(groupId);

        if (segmentInfo != null) {
            return segmentInfo.lastLogIndexExclusive();
        }

        return indexFileManager.lastLogIndexExclusive(groupId);
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

    private EntrySearchResult readFromOtherSegmentFiles(long groupId, long logIndex, int attemptNum) throws IOException {
        SegmentFilePointer segmentFilePointer = indexFileManager.getSegmentFilePointer(groupId, logIndex);

        if (segmentFilePointer == null) {
            return EntrySearchResult.notFound();
        }

        Path path = segmentFilesDir.resolve(SegmentFile.fileName(segmentFilePointer.fileProperties()));

        // TODO: Add a cache for recently accessed segment files, see https://issues.apache.org/jira/browse/IGNITE-26622.
        try {
            SegmentFile segmentFile = SegmentFile.openExisting(path, isSync);

            ByteBuffer buffer = segmentFile.buffer().position(segmentFilePointer.payloadOffset());

            return EntrySearchResult.success(buffer);
        } catch (FileNotFoundException e) {
            // When reading from a segment file based on information from the index manager, there exists a race with the Garbage Collector:
            // index manager can return a pointer to a segment file that may have been compacted. In this case, we should just retry and
            // get the more recent information.
            if (attemptNum == MAX_NUM_INDEX_FILE_READ_RETRIES) {
                throw e;
            }

            LOG.info("Segment file {} not found, retrying (attempt {}/{}).", path, attemptNum, MAX_NUM_INDEX_FILE_READ_RETRIES - 1);

            return readFromOtherSegmentFiles(groupId, logIndex, attemptNum + 1);
        }
    }

    private static int maxLogEntrySize(LogStorageView storageConfiguration) {
        int valueFromConfig = storageConfiguration.maxLogEntrySizeBytes();

        if (valueFromConfig != UNSPECIFIED_MAX_LOG_ENTRY_SIZE) {
            return valueFromConfig;
        }

        return computeDefaultMaxLogEntrySizeBytes(storageConfiguration.segmentFileSizeBytes());
    }
}
