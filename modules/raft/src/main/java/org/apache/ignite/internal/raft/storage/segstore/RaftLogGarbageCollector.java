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
import static org.apache.ignite.internal.raft.storage.segstore.IndexFileManager.indexFileProperties;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentFileManager.HEADER_RECORD;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayload.CRC_SIZE_BYTES;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayloadParser.endOfSegmentReached;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayloadParser.validateSegmentFileHeader;
import static org.apache.ignite.internal.util.IgniteUtils.atomicMoveFile;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.storage.segstore.GroupInfoProvider.GroupInfo;
import org.apache.ignite.internal.raft.util.VarlenEncoder;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Garbage Collector for Raft log segment files.
 *
 * <p>The garbage collector performs compaction of segment files by removing truncated log entries and creating new generations
 * of segment files. This process reclaims disk space occupied by log entries that have been truncated via
 * {@link LogStorage#truncatePrefix} or {@link LogStorage#truncateSuffix} operations.
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

    private final GroupInfoProvider groupInfoProvider;

    private final AtomicLong logSize = new AtomicLong();

    RaftLogGarbageCollector(
            Path segmentFilesDir,
            IndexFileManager indexFileManager,
            GroupInfoProvider groupInfoProvider
    ) {
        this.segmentFilesDir = segmentFilesDir;
        this.indexFileManager = indexFileManager;
        this.groupInfoProvider = groupInfoProvider;
    }

    void cleanupLeftoverFiles() throws IOException {
        FileProperties prevFileProperties = null;

        try (Stream<Path> segmentFiles = Files.list(segmentFilesDir)) {
            Iterator<Path> it = segmentFiles.sorted().iterator();

            while (it.hasNext()) {
                Path segmentFile = it.next();

                if (segmentFile.getFileName().toString().endsWith(TMP_FILE_SUFFIX)) {
                    LOG.info("Deleting temporary segment file: {}.", segmentFile);

                    Files.delete(segmentFile);
                } else {
                    FileProperties fileProperties = SegmentFile.fileProperties(segmentFile);

                    if (prevFileProperties != null && prevFileProperties.ordinal() == fileProperties.ordinal()) {
                        Path prevPath = segmentFilesDir.resolve(SegmentFile.fileName(prevFileProperties));

                        LOG.info("Deleting segment file {} because it has a higher generation version.", prevPath);

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

                // Temporary index fils are not created by the GC, they are created by the index manager and are cleaned up by it.
                if (!Files.exists(segmentFilesDir.resolve(SegmentFile.fileName(fileProperties)))) {
                    LOG.info("Deleting index file {} because the corresponding segment file does not exist.", indexFile);

                    Files.delete(indexFile);
                } else if (prevFileProperties != null && prevFileProperties.ordinal() == fileProperties.ordinal()) {
                    Path prevPath = indexFileManager.indexFilePath(prevFileProperties);

                    LOG.info("Deleting index file {} because it has a higher generation version.", prevPath);

                    Files.deleteIfExists(prevPath);
                }

                prevFileProperties = fileProperties;
            }
        }
    }

    // TODO: Optimize compaction of completely truncated files, see https://issues.apache.org/jira/browse/IGNITE-27964.
    @VisibleForTesting
    void compactSegmentFile(SegmentFile segmentFile) throws IOException {
        LOG.info("Compacting segment file: {}.", segmentFile.path());

        // Cache for avoiding excessive min/max log index computations.
        var logStorageInfos = new Long2ObjectOpenHashMap<GroupInfo>();

        ByteBuffer buffer = segmentFile.buffer();

        validateSegmentFileHeader(buffer, segmentFile.path());

        TmpSegmentFile tmpSegmentFile = null;

        WriteModeIndexMemTable tmpMemTable = null;

        try {
            while (!endOfSegmentReached(buffer)) {
                int originalStartOfRecordOffset = buffer.position();

                long groupId = buffer.getLong();

                int payloadLength = buffer.getInt();

                if (payloadLength <= 0) {
                    // Skip special entries (such as truncation records). They can always be omitted.
                    // To identify such entries we rely on the fact that they have negative length field value.
                    int endOfRecordOffset = buffer.position() + Long.BYTES + CRC_SIZE_BYTES;

                    buffer.position(endOfRecordOffset);

                    continue;
                }

                int endOfRecordOffset = buffer.position() + payloadLength + CRC_SIZE_BYTES;

                long index = VarlenEncoder.readLong(buffer);

                GroupInfo info = logStorageInfos.computeIfAbsent(groupId, groupInfoProvider::groupInfo);

                if (info == null || index < info.firstLogIndexInclusive() || index >= info.lastLogIndexExclusive()) {
                    // We found a truncated entry, it should be skipped.
                    buffer.position(endOfRecordOffset);

                    continue;
                }

                if (tmpSegmentFile == null) {
                    tmpSegmentFile = new TmpSegmentFile(segmentFile);

                    tmpSegmentFile.writeHeader();

                    tmpMemTable = new SingleThreadMemTable();
                }

                int oldLimit = buffer.limit();

                // Set the buffer boundaries to only write the current record to the new file.
                buffer.position(originalStartOfRecordOffset).limit(endOfRecordOffset);

                @SuppressWarnings("resource")
                long newStartOfRecordOffset = tmpSegmentFile.fileChannel().position();

                writeFully(tmpSegmentFile.fileChannel(), buffer);

                buffer.limit(oldLimit);

                tmpMemTable.appendSegmentFileOffset(groupId, index, toIntExact(newStartOfRecordOffset));
            }

            Path indexFilePath = indexFileManager.indexFilePath(segmentFile.fileProperties());

            long logSizeDelta;

            if (tmpSegmentFile != null) {
                tmpSegmentFile.syncAndRename();

                // Create a new index file and update the in-memory state to point to it.
                Path newIndexFilePath = indexFileManager.onIndexFileCompacted(
                        tmpMemTable.transitionToReadMode(),
                        segmentFile.fileProperties(),
                        tmpSegmentFile.fileProperties()
                );

                logSizeDelta = Files.size(segmentFile.path())
                        + Files.size(indexFilePath)
                        - tmpSegmentFile.size()
                        - Files.size(newIndexFilePath);
            } else {
                // We got lucky and the whole file can be removed.
                logSizeDelta = Files.size(segmentFile.path()) + Files.size(indexFilePath);
            }

            // Remove the previous generation of the segment file and its index. This is safe to do, because we rely on the file system
            // guarantees that other threads reading from the segment file will still be able to do that even if the file is deleted.
            Files.delete(segmentFile.path());
            Files.delete(indexFilePath);

            long newLogSize = logSize.addAndGet(-logSizeDelta);

            if (LOG.isInfoEnabled()) {
                if (tmpSegmentFile == null) {
                    LOG.info(
                            "Segment file {} removed (all entries are truncated). Log size freed: {}. New log size: {}.",
                            segmentFile.path(), logSizeDelta, newLogSize
                    );
                } else {
                    LOG.info(
                            "Segment file {} compacted. Log size freed: {}. New log size: {}.",
                            segmentFile.path(), logSizeDelta, newLogSize
                    );
                }
            }
        } finally {
            if (tmpSegmentFile != null) {
                tmpSegmentFile.close();
            }
        }
    }

    private static void writeFully(WritableByteChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private class TmpSegmentFile implements ManuallyCloseable {
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
