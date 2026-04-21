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
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.internal.raft.storage.segstore.IndexFileManager.indexFileProperties;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentFileManager.HEADER_RECORD;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayload.CRC_SIZE_BYTES;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayload.RESET_RECORD_MARKER;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayload.TRUNCATE_PREFIX_RECORD_MARKER;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayload.TRUNCATE_SUFFIX_RECORD_MARKER;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayloadParser.endOfSegmentReached;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayloadParser.validateSegmentFileHeader;
import static org.apache.ignite.internal.util.IgniteUtils.atomicMoveFile;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.util.VarlenEncoder;
import org.apache.ignite.internal.thread.IgniteThread;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Garbage Collector for Raft log segment files.
 *
 * <p>The garbage collector performs compaction of segment files by removing truncated log entries and creating new generations
 * of segment files. This process reclaims disk space occupied by log entries that have been truncated via {@link LogStorage#truncatePrefix}
 * or {@link LogStorage#truncateSuffix} operations.
 *
 * <h2>Size tracking</h2>
 * The GC tracks the total size of all log storage files (segment files and index files) via an {@link AtomicLong}. The counter is
 * incremented via {@link #onLogStorageSizeIncreased(long)} before a new segment file is allocated or before an index file is written to
 * disk, and decremented inside {@link #runCompaction} after files are deleted. The GC thread wakes up on each of these events and compacts
 * files until the total size drops below the configured {@link #softLimitBytes soft limit}.
 *
 * <h2>Compaction Process</h2>
 * When a segment file is selected for compaction, the GC:
 * <ol>
 *     <li>Copies non-truncated entries to a new segment file with an incremented generation number</li>
 *     <li>Creates a new index file for the new generation</li>
 *     <li>Atomically replaces the old segment file with the new one</li>
 *     <li>Deletes the old segment file and its index file</li>
 * </ol>
 */
class RaftLogGarbageCollector {
    private static final IgniteLogger LOG = Loggers.forClass(RaftLogGarbageCollector.class);

    private static final String TMP_FILE_SUFFIX = ".tmp";

    private final Path segmentFilesDir;

    private final IndexFileManager indexFileManager;

    private final long softLimitBytes;

    private final SegmentFileCompactionStrategy strategy;

    private final FailureProcessor failureProcessor;

    private final AtomicLong logSizeBytes = new AtomicLong();

    private final Thread gcThread;

    RaftLogGarbageCollector(
            String nodeName,
            String storageName,
            Path segmentFilesDir,
            IndexFileManager indexFileManager,
            long softLimitBytes,
            SegmentFileCompactionStrategy strategy,
            FailureProcessor failureProcessor
    ) {
        this.segmentFilesDir = segmentFilesDir;
        this.indexFileManager = indexFileManager;
        this.softLimitBytes = softLimitBytes;
        this.strategy = strategy;
        this.failureProcessor = failureProcessor;

        gcThread = new IgniteThread(nodeName, "segstore-gc-" + storageName, new GcTask());
    }

    void start() throws IOException {
        initLogSizeFromDisk();

        gcThread.start();
    }

    void stop() {
        gcThread.interrupt();

        try {
            gcThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInternalException(INTERNAL_ERR, "Interrupted while waiting for the GC thread to finish.", e);
        }
    }

    /**
     * Notifies the GC that the log storage size is about to increase by {@code addedBytes} and wakes the GC thread if the soft limit is now
     * exceeded. Must be called before the corresponding file is written to disk.
     */
    void onLogStorageSizeIncreased(long addedBytes) {
        long logSize = logSizeBytes.addAndGet(addedBytes);

        if (logSize >= softLimitBytes) {
            LOG.info(
                    "Log size is above the soft limit, waking up GC thread [current log size = {} bytes, soft limit = {} bytes].",
                    logSize, softLimitBytes
            );

            LockSupport.unpark(gcThread);
        }
    }

    void cleanupLeftoverFiles() throws IOException {
        FileProperties prevFileProperties = null;

        try (Stream<Path> segmentFiles = Files.list(segmentFilesDir)) {
            Iterator<Path> it = segmentFiles.sorted().iterator();

            while (it.hasNext()) {
                Path segmentFile = it.next();

                if (segmentFile.getFileName().toString().endsWith(TMP_FILE_SUFFIX)) {
                    LOG.info("Deleting temporary segment file [path = {}].", segmentFile);

                    Files.delete(segmentFile);
                } else {
                    FileProperties fileProperties = SegmentFile.fileProperties(segmentFile);

                    if (prevFileProperties != null && prevFileProperties.ordinal() == fileProperties.ordinal()) {
                        Path prevPath = segmentFilesDir.resolve(SegmentFile.fileName(prevFileProperties));

                        LOG.info("Deleting segment file because it has a higher generation version [path = {}].", prevPath);

                        Files.delete(prevPath);
                    }

                    prevFileProperties = fileProperties;
                }
            }
        }

        // Do the same routine but for index files.
        prevFileProperties = null;

        try (Stream<Path> indexFiles = Files.list(indexFileManager.indexFilesDir())) {
            Iterator<Path> it = indexFiles.sorted().iterator();

            while (it.hasNext()) {
                Path indexFile = it.next();

                FileProperties fileProperties = indexFileProperties(indexFile);

                // The GC does not create temporary index files, they are created by the index manager and are cleaned up by it.
                if (!Files.exists(segmentFilesDir.resolve(SegmentFile.fileName(fileProperties)))) {
                    LOG.info("Deleting index file because the corresponding segment file does not exist [path = {}].", indexFile);

                    Files.delete(indexFile);
                } else if (prevFileProperties != null && prevFileProperties.ordinal() == fileProperties.ordinal()) {
                    Path prevPath = indexFileManager.indexFilePath(prevFileProperties);

                    LOG.info("Deleting index file because it has a higher generation version [path = {}].", prevPath);

                    Files.deleteIfExists(prevPath);
                }

                prevFileProperties = fileProperties;
            }
        }
    }

    /** Returns current size of all log storage files in bytes. */
    long logSizeBytes() {
        return logSizeBytes.get();
    }

    @VisibleForTesting
    void runCompaction(FileProperties segmentFileProperties) throws IOException {
        Path segmentFilePath = segmentFilesDir.resolve(SegmentFile.fileName(segmentFileProperties));

        LOG.info("Compacting segment file [path = {}].", segmentFilePath);

        // TODO: Skip non-compactible files, see https://issues.apache.org/jira/browse/IGNITE-28417.
        Long2ObjectMap<IndexFileMeta> segmentFileDescription
                = indexFileManager.describeSegmentFile(segmentFileProperties.ordinal());

        boolean canRemoveSegmentFile = segmentFileDescription.isEmpty();

        Path indexFilePath = indexFileManager.indexFilePath(segmentFileProperties);

        long logSizeDelta;

        if (canRemoveSegmentFile) {
            indexFileManager.onIndexFileRemoved(segmentFileProperties);

            logSizeDelta = Files.size(segmentFilePath) + Files.size(indexFilePath);
        } else {
            SegmentFile segmentFile = SegmentFile.openExisting(segmentFilePath, false);

            try {
                logSizeDelta = compactSegmentFile(segmentFile, indexFilePath, segmentFileDescription);
            } finally {
                segmentFile.close();
            }
        }

        // Remove the previous generation of the segment file and its index. This is safe to do, because we rely on the file system
        // guarantees that other threads reading from the segment file will still be able to do that even if the file is deleted.
        Files.delete(segmentFilePath);
        Files.delete(indexFilePath);

        long newLogSize = logSizeBytes.addAndGet(-logSizeDelta);

        if (LOG.isInfoEnabled()) {
            if (canRemoveSegmentFile) {
                LOG.info(
                        "Segment file removed (all entries are truncated) [path = {}, log size freed = {} bytes, new log size = {} bytes].",
                        segmentFilePath, logSizeDelta, newLogSize
                );
            } else {
                LOG.info(
                        "Segment file compacted [path = {}, log size freed = {} bytes, new log size = {} bytes].",
                        segmentFilePath, logSizeDelta, newLogSize
                );
            }
        }
    }

    private long compactSegmentFile(
            SegmentFile segmentFile,
            Path indexFilePath,
            Long2ObjectMap<IndexFileMeta> segmentFileDescription
    ) throws IOException {
        ByteBuffer buffer = segmentFile.buffer();

        validateSegmentFileHeader(buffer, segmentFile.path());

        try (var tmpSegmentFile = new TmpSegmentFile(segmentFile)) {
            tmpSegmentFile.writeHeader();

            var tmpMemTable = new SingleThreadMemTable();

            while (!endOfSegmentReached(buffer)) {
                int originalStartOfRecordOffset = buffer.position();

                long groupId = buffer.getLong();

                int payloadLength = buffer.getInt();

                if (payloadLength <= 0) {
                    switch (payloadLength) {
                        case TRUNCATE_SUFFIX_RECORD_MARKER:
                            long lastLogIndexKept = buffer.getLong();

                            tmpMemTable.truncateSuffix(groupId, lastLogIndexKept);

                            break;
                        case TRUNCATE_PREFIX_RECORD_MARKER:
                            long firstLogIndexKept = buffer.getLong();

                            tmpMemTable.truncatePrefix(groupId, firstLogIndexKept);

                            break;

                        case RESET_RECORD_MARKER:
                            long nextLogIndex = buffer.getLong();

                            tmpMemTable.reset(groupId, nextLogIndex);

                            break;
                        default:
                            throw new IllegalStateException(String.format("Unknown record marker [payloadLength = %d].]", payloadLength));
                    }

                    buffer.position(buffer.position() + CRC_SIZE_BYTES);

                    continue;
                }

                int endOfRecordOffset = buffer.position() + payloadLength + CRC_SIZE_BYTES;

                long index = VarlenEncoder.readLong(buffer);

                IndexFileMeta indexFileMeta = segmentFileDescription.get(groupId);

                if (indexFileMeta == null || !isLogIndexInRange(index, indexFileMeta)) {
                    // We found a truncated entry, it should be skipped.
                    buffer.position(endOfRecordOffset);

                    continue;
                }

                int oldLimit = buffer.limit();

                // Set the buffer boundaries to only write the current record to the new file.
                buffer.position(originalStartOfRecordOffset).limit(endOfRecordOffset);

                long newStartOfRecordOffset = tmpSegmentFile.fileChannel().position();

                writeFully(tmpSegmentFile.fileChannel(), buffer);

                buffer.limit(oldLimit);

                tmpMemTable.appendSegmentFileOffset(groupId, index, toIntExact(newStartOfRecordOffset));
            }

            assert tmpMemTable.numGroups() != 0
                    : String.format("All entries have been truncated, this should not happen [path = %s].", segmentFile.path());

            tmpSegmentFile.syncAndRename();

            // Create a new index file and update the in-memory state to point to it.
            Path newIndexFilePath = indexFileManager.onIndexFileCompacted(
                    tmpMemTable.transitionToReadMode(),
                    segmentFile.fileProperties(),
                    tmpSegmentFile.fileProperties()
            );

            return Files.size(segmentFile.path())
                    + Files.size(indexFilePath)
                    - tmpSegmentFile.size()
                    - Files.size(newIndexFilePath);
        }
    }

    private static void writeFully(WritableByteChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private static boolean isLogIndexInRange(long index, IndexFileMeta indexFileMeta) {
        return index >= indexFileMeta.firstLogIndexInclusive() && index < indexFileMeta.lastLogIndexExclusive();
    }

    private void initLogSizeFromDisk() throws IOException {
        Path indexFilesDir = indexFileManager.indexFilesDir();

        try (Stream<Path> files = Stream.concat(Files.list(segmentFilesDir), Files.list(indexFilesDir))) {
            long logSizeOnDisk = files
                    .mapToLong(path -> {
                        try {
                            return Files.size(path);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    })
                    .sum();

            logSizeBytes.addAndGet(logSizeOnDisk);
        }
    }

    private class GcTask implements Runnable {
        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    runGcCycle();
                } catch (ClosedByInterruptException e) {
                    return;
                } catch (IOException e) {
                    failureProcessor.process(new FailureContext(CRITICAL_ERROR, e));
                }

                LockSupport.park();
            }
        }

        private void runGcCycle() throws IOException {
            if (logSizeBytes.get() < softLimitBytes) {
                return;
            }

            LOG.info("Starting Log Storage GC cycle.");

            try (Stream<FileProperties> candidates = strategy.selectCandidates()) {
                Iterator<FileProperties> it = candidates.iterator();

                do {
                    if (!it.hasNext()) {
                        LOG.warn(
                                "Log size is above the soft limit but there are no files to compact "
                                        + "[currentLogSize = {} bytes, softLimit = {} bytes].",
                                logSizeBytes.get(), softLimitBytes
                        );

                        return;
                    }

                    runCompaction(it.next());
                } while (logSizeBytes.get() >= softLimitBytes);
            }

            LOG.info("Log Storage GC cycle complete.");
        }
    }

    private class TmpSegmentFile implements AutoCloseable {
        private final String fileName;

        private final Path tmpFilePath;

        private final FileChannel fileChannel;

        private final FileProperties fileProperties;

        TmpSegmentFile(SegmentFile originalFile) throws IOException {
            FileProperties originalFileProperties = originalFile.fileProperties();

            this.fileProperties = new FileProperties(originalFileProperties.ordinal(), originalFileProperties.generation() + 1);
            this.fileName = SegmentFile.fileName(fileProperties);
            this.tmpFilePath = segmentFilesDir.resolve(fileName + TMP_FILE_SUFFIX);
            this.fileChannel = FileChannel.open(tmpFilePath, CREATE_NEW, WRITE);
        }

        void writeHeader() throws IOException {
            fileChannel.write(ByteBuffer.wrap(HEADER_RECORD));
        }

        FileChannel fileChannel() {
            return fileChannel;
        }

        void syncAndRename() throws IOException {
            fileChannel.force(true);

            atomicMoveFile(tmpFilePath, tmpFilePath.resolveSibling(fileName), LOG);
        }

        long size() throws IOException {
            return fileChannel.size();
        }

        FileProperties fileProperties() {
            return fileProperties;
        }

        @Override
        public void close() throws IOException {
            fileChannel.close();
        }
    }
}
