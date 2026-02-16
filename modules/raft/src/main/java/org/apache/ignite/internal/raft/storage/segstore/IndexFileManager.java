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
import static org.apache.ignite.internal.util.IgniteUtils.atomicMoveFile;
import static org.apache.ignite.internal.util.IgniteUtils.fsyncFile;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.jetbrains.annotations.Nullable;

/**
 * File manager responsible for persisting {@link ReadModeIndexMemTable}s to index files.
 *
 * <p>When a checkpoint is triggered on segment file rollover, the current index memtable is scheduled for being saved to a file. The
 * format of this file is as follows:
 * <pre>
 * +--------+---------------------+-----+---------------------+
 * | Header | Payload for group 1 | ... | Payload for group N |
 * +--------+---------------------+-----+---------------------+
 * </pre>
 *
 * <p>Header structure consists of a common meta and a meta for each Raft group present in the memtable. The common meta is as follows:
 * <pre>
 * +------------------------------------------------------------------+
 * |                       Common meta                                |
 * +------------------------------------------------------------------+
 * | Magic (4 bytes) | Version (4 bytes) | Number of groups (4 bytes) |
 * +------------------------------------------------------------------+
 * </pre>
 *
 * <p>Each Raft group meta is as follows (written as a list, because the table doesn't fit the configured line length):
 * <ol>
 *     <li>Group ID (8 bytes);</li>
 *     <li>Flags (4 bytes);</li>
 *     <li>Payload offset (4 bytes);</li>
 *     <li>First log index (8 bytes, inclusive);</li>
 *     <li>Last log index (8 bytes, exclusive);</li>
 *     <li>First log index kept (8 bytes): used during prefix truncation, either equal to first index kept if prefix was truncated at least
 *     once during the index file lifecycle, otherwise equal to {@code -1}.</li>
 * </ol>
 *
 * <p>Payload of the index files has the following structure:
 * <pre>
 * +-------------------------------------------------------------------------+-----+
 * |                           Payload for group 1                           | ... |
 * +-------------------------------------------------------------------------+-----+
 * | Segment file offset 1 (4 bytes) | ... | Segment file offset N (4 bytes) | ... |
 * +-------------------------------------------------------------------------+-----+
 * </pre>
 *
 * <p>Index File Manager is also responsible for maintaining an in-memory cache of persisted index files' metadata for quicker index file
 * lookup.
 *
 * @see ReadModeIndexMemTable
 * @see SegmentFileManager
 */
class IndexFileManager {
    /**
     * Maximum number of times we try to read data from a segment file returned based the index before giving up and throwing an
     * exception. See {@link #getSegmentFilePointer} for more information.
     */
    private static final int MAX_NUM_INDEX_FILE_READ_RETRIES = 5;

    private static final IgniteLogger LOG = Loggers.forClass(IndexFileManager.class);

    static final int MAGIC_NUMBER = 0x6BF0A76A;

    static final int FORMAT_VERSION = 1;

    private static final String INDEX_FILE_NAME_FORMAT = "index-%010d-%010d.bin";

    private static final Pattern INDEX_FILE_NAME_PATTERN = Pattern.compile("index-(?<ordinal>\\d{10})-(?<generation>\\d{10})\\.bin");

    private static final String TMP_FILE_SUFFIX = ".tmp";

    /** Size of the segment file offset entry (used as the payload of an index file). */
    static final int SEGMENT_FILE_OFFSET_SIZE = Integer.BYTES;

    // Magic number + format version + number of Raft groups.
    static final int COMMON_META_SIZE = Integer.BYTES + Integer.BYTES + Integer.BYTES;

    // Group ID + flags + file offset + start log index + end log index + first index kept.
    static final int GROUP_META_SIZE = Long.BYTES + Integer.BYTES + Integer.BYTES + Long.BYTES + Long.BYTES + Long.BYTES;

    static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    private final Path indexFilesDir;

    /**
     * Current index file ordinal (used to generate index file names).
     *
     * <p>No synchronized access is needed because this field is only used by the checkpoint thread and during startup.
     *
     * <p>{@code -1} means that the manager has not been started yet.
     */
    private int curFileOrdinal = -1;

    /**
     * Index file metadata grouped by Raft Group ID.
     */
    // FIXME: This map is never cleaned up, see https://issues.apache.org/jira/browse/IGNITE-27926.
    private final Map<Long, GroupIndexMeta> groupIndexMetas = new ConcurrentHashMap<>();

    IndexFileManager(Path baseDir) throws IOException {
        indexFilesDir = baseDir.resolve("index");

        Files.createDirectories(indexFilesDir);
    }

    void start() throws IOException {
        try (Stream<Path> indexFiles = Files.list(indexFilesDir)) {
            Iterator<Path> it = indexFiles.sorted().iterator();

            while (it.hasNext()) {
                recoverIndexFileMetas(it.next());
            }
        }
    }

    Path indexFilesDir() {
        return indexFilesDir;
    }

    void cleanupLeftoverFiles() throws IOException {
        try (Stream<Path> indexFiles = Files.list(indexFilesDir)) {
            Iterator<Path> it = indexFiles.iterator();

            while (it.hasNext()) {
                Path indexFile = it.next();

                if (indexFile.getFileName().toString().endsWith(TMP_FILE_SUFFIX)) {
                    LOG.info("Deleting temporary index file: {}.", indexFile);

                    Files.delete(indexFile);
                }
            }
        }
    }

    /**
     * Saves the given index memtable to a file.
     *
     * <p>Must only be called by the checkpoint thread.
     */
    Path saveNewIndexMemtable(ReadModeIndexMemTable indexMemTable) throws IOException {
        var newFileProperties = new FileProperties(++curFileOrdinal);

        Path indexFilePath = indexFilePath(newFileProperties);

        List<IndexMetaSpec> metaSpecs = saveIndexMemtable(indexFilePath, indexMemTable, newFileProperties);

        metaSpecs.forEach(this::putIndexFileMeta);

        return indexFilePath;
    }

    private List<IndexMetaSpec> saveIndexMemtable(
            Path indexFilePath,
            ReadModeIndexMemTable indexMemTable,
            FileProperties fileProperties
    ) throws IOException {
        String fileName = indexFilePath.getFileName().toString();

        Path tmpFilePath = indexFilesDir.resolve(fileName + TMP_FILE_SUFFIX);

        assert !Files.exists(indexFilesDir.resolve(fileName)) : "Index file already exists: " + fileName;
        assert !Files.exists(tmpFilePath) : "Temporary index file already exists: " + tmpFilePath;

        FileHeaderWithIndexMetas fileHeaderWithIndexMetas = serializeHeaderAndFillMetadata(indexMemTable, fileProperties);

        try (var os = new BufferedOutputStream(Files.newOutputStream(tmpFilePath, CREATE_NEW, WRITE))) {
            os.write(fileHeaderWithIndexMetas.header());

            Iterator<Entry<Long, SegmentInfo>> it = indexMemTable.iterator();

            while (it.hasNext()) {
                SegmentInfo segmentInfo = it.next().getValue();

                // Segment Info may not contain payload in case of suffix truncation, see "IndexMemTable#truncateSuffix".
                if (segmentInfo.size() > 0) {
                    os.write(payload(segmentInfo));
                }
            }
        }

        syncAndRename(tmpFilePath, tmpFilePath.resolveSibling(fileName));

        return fileHeaderWithIndexMetas.indexMetas();
    }

    /**
     * This method is intended to be called during {@link SegmentFileManager} recovery in order to create index files that may have been
     * lost due to a component stop before a checkpoint was able to complete.
     */
    void recoverIndexFile(ReadModeIndexMemTable indexMemTable, FileProperties fileProperties) throws IOException {
        // On recovery we are only creating missing index files, in-memory meta will be created on Index File Manager start.
        // (see recoverIndexFileMetas).
        saveIndexMemtable(indexFilePath(fileProperties), indexMemTable, fileProperties);
    }

    void onIndexFileCompacted(
            ReadModeIndexMemTable indexMemTable,
            FileProperties oldIndexFileProperties,
            FileProperties newIndexFileProperties
    ) throws IOException {
        Path newIndexFilePath = indexFilePath(newIndexFileProperties);

        List<IndexMetaSpec> metaSpecs = saveIndexMemtable(newIndexFilePath, indexMemTable, newIndexFileProperties);

        metaSpecs.forEach(metaSpec -> {
            GroupIndexMeta groupIndexMeta = groupIndexMetas.get(metaSpec.groupId);

            IndexFileMeta meta = metaSpec.indexFileMeta();

            if (groupIndexMeta != null && meta != null) {
                groupIndexMeta.onIndexCompacted(oldIndexFileProperties, meta);
            }
        });

        LOG.info("New index file created after compaction: {}.", newIndexFilePath);
    }

    /**
     * Returns a pointer into a segment file that contains the entry for the given group's index. Returns {@code null} if the given log
     * index could not be found in any of the index files.
     */
    @Nullable
    SegmentFilePointer getSegmentFilePointer(long groupId, long logIndex) throws IOException {
        return getSegmentFilePointer(groupId, logIndex, 1);
    }

    @Nullable
    private SegmentFilePointer getSegmentFilePointer(long groupId, long logIndex, int attemptNum) throws IOException {
        GroupIndexMeta groupIndexMeta = groupIndexMetas.get(groupId);

        if (groupIndexMeta == null) {
            return null;
        }

        IndexFileMeta indexFileMeta = groupIndexMeta.indexMeta(logIndex);

        if (indexFileMeta == null) {
            return null;
        }

        Path indexFile = indexFilesDir.resolve(indexFileName(indexFileMeta.indexFileProperties()));

        try (SeekableByteChannel channel = Files.newByteChannel(indexFile, StandardOpenOption.READ)) {
            // Index file payload is a 0-based array, which indices correspond to the [fileMeta.firstLogIndex, fileMeta.lastLogIndex) range.
            long payloadArrayIndex = logIndex - indexFileMeta.firstLogIndexInclusive();

            assert payloadArrayIndex >= 0 : payloadArrayIndex;

            long payloadOffset = indexFileMeta.indexFilePayloadOffset() + payloadArrayIndex * Integer.BYTES;

            channel.position(payloadOffset);

            ByteBuffer segmentPayloadOffsetBuffer = ByteBuffer.allocate(Integer.BYTES).order(BYTE_ORDER);

            while (segmentPayloadOffsetBuffer.hasRemaining()) {
                int bytesRead = channel.read(segmentPayloadOffsetBuffer);

                if (bytesRead == -1) {
                    throw new EOFException("EOF reached while reading index file: " + indexFile);
                }
            }

            int segmentPayloadOffset = segmentPayloadOffsetBuffer.getInt(0);

            return new SegmentFilePointer(indexFileMeta.indexFileProperties(), segmentPayloadOffset);
        } catch (NoSuchFileException e) {
            // There exists a race between the Garbage Collection process and "groupIndexMetas.get" call. It is possible that
            // groupIndexMetas returned stale information, we should simply retry in this case.
            if (attemptNum == MAX_NUM_INDEX_FILE_READ_RETRIES) {
                throw e;
            }

            LOG.info("Index file {} not found, retrying (attempt {}/{}).", indexFile, attemptNum, MAX_NUM_INDEX_FILE_READ_RETRIES - 1);

            return getSegmentFilePointer(groupId, logIndex, attemptNum + 1);
        }
    }

    /**
     * Returns the lowest log index for the given group across all index files or {@code -1} if no such index exists.
     */
    long firstLogIndexInclusive(long groupId) {
        GroupIndexMeta groupIndexMeta = groupIndexMetas.get(groupId);

        return groupIndexMeta == null ? -1 : groupIndexMeta.firstLogIndexInclusive();
    }

    /**
     * Returns the highest possible log index for the given group across all index files or {@code -1} if no such index exists.
     */
    long lastLogIndexExclusive(long groupId) {
        GroupIndexMeta groupIndexMeta = groupIndexMetas.get(groupId);

        return groupIndexMeta == null ? -1 : groupIndexMeta.lastLogIndexExclusive();
    }

    Path indexFilePath(FileProperties fileProperties) {
        return indexFilesDir.resolve(indexFileName(fileProperties));
    }

    private static FileHeaderWithIndexMetas serializeHeaderAndFillMetadata(
            ReadModeIndexMemTable indexMemTable,
            FileProperties fileProperties
    ) {
        int numGroups = indexMemTable.numGroups();

        int headerSize = headerSize(numGroups);

        ByteBuffer headerBuffer = ByteBuffer.allocate(headerSize)
                .order(BYTE_ORDER)
                .putInt(MAGIC_NUMBER)
                .putInt(FORMAT_VERSION)
                .putInt(numGroups);

        int payloadOffset = headerSize;

        var metaSpecs = new ArrayList<IndexMetaSpec>(numGroups);

        Iterator<Entry<Long, SegmentInfo>> it = indexMemTable.iterator();

        while (it.hasNext()) {
            Entry<Long, SegmentInfo> entry = it.next();

            // Using the boxed value to avoid unnecessary autoboxing later.
            Long groupId = entry.getKey();

            SegmentInfo segmentInfo = entry.getValue();

            long firstLogIndexInclusive = segmentInfo.firstLogIndexInclusive();

            long lastLogIndexExclusive = segmentInfo.lastLogIndexExclusive();

            long firstIndexKept = segmentInfo.firstIndexKept();

            IndexFileMeta indexFileMeta = createIndexFileMeta(
                    firstLogIndexInclusive, lastLogIndexExclusive, firstIndexKept, payloadOffset, fileProperties
            );

            metaSpecs.add(new IndexMetaSpec(groupId, indexFileMeta, firstIndexKept));

            headerBuffer
                    .putLong(groupId)
                    .putInt(0) // Flags.
                    .putInt(payloadOffset)
                    .putLong(firstLogIndexInclusive)
                    .putLong(lastLogIndexExclusive)
                    .putLong(firstIndexKept);

            payloadOffset += payloadSize(segmentInfo);
        }

        return new FileHeaderWithIndexMetas(headerBuffer.array(), metaSpecs);
    }

    private static @Nullable IndexFileMeta createIndexFileMeta(
            long firstLogIndexInclusive,
            long lastLogIndexExclusive,
            long firstIndexKept,
            int payloadOffset,
            FileProperties fileProperties
    ) {
        if (firstLogIndexInclusive == -1) {
            assert firstIndexKept != -1 : "Expected a prefix tombstone, but firstIndexKept is not set.";

            // This is a "prefix tombstone", no need to create any meta, we will just truncate the prefix.
            return null;
        }

        if (firstIndexKept == -1 || firstIndexKept <= firstLogIndexInclusive) {
            // No prefix truncation required, simply create a new meta.
            return new IndexFileMeta(firstLogIndexInclusive, lastLogIndexExclusive, payloadOffset, fileProperties);
        }

        // Create a meta with a truncated prefix.
        int numEntriesToSkip = toIntExact(firstIndexKept - firstLogIndexInclusive);

        int adjustedPayloadOffset = payloadOffset + numEntriesToSkip * SEGMENT_FILE_OFFSET_SIZE;

        return new IndexFileMeta(firstIndexKept, lastLogIndexExclusive, adjustedPayloadOffset, fileProperties);
    }

    private void putIndexFileMeta(IndexMetaSpec metaSpec) {
        IndexFileMeta indexFileMeta = metaSpec.indexFileMeta();

        Long groupId = metaSpec.groupId();

        long firstIndexKept = metaSpec.firstIndexKept();

        GroupIndexMeta existingGroupIndexMeta = groupIndexMetas.get(groupId);

        if (existingGroupIndexMeta == null) {
            if (indexFileMeta != null) {
                groupIndexMetas.put(groupId, new GroupIndexMeta(indexFileMeta));
            }
        } else {
            if (firstIndexKept != -1) {
                existingGroupIndexMeta.truncatePrefix(firstIndexKept);
            }

            if (indexFileMeta != null) {
                // New index meta must have already been truncated according to the prefix tombstone.
                assert indexFileMeta.firstLogIndexInclusive() >= firstIndexKept : indexFileMeta;

                existingGroupIndexMeta.addIndexMeta(indexFileMeta);
            }
        }
    }

    private static Path syncAndRename(Path from, Path to) throws IOException {
        fsyncFile(from);

        return atomicMoveFile(from, to, LOG);
    }

    private static byte[] payload(SegmentInfo segmentInfo) {
        ByteBuffer payloadBuffer = ByteBuffer.allocate(payloadSize(segmentInfo)).order(BYTE_ORDER);

        segmentInfo.saveOffsetsTo(payloadBuffer);

        return payloadBuffer.array();
    }

    private static int headerSize(int numGroups) {
        return COMMON_META_SIZE + numGroups * GROUP_META_SIZE;
    }

    private static int payloadSize(SegmentInfo segmentInfo) {
        return segmentInfo.size() * Integer.BYTES;
    }

    private static String indexFileName(FileProperties fileProperties) {
        return String.format(INDEX_FILE_NAME_FORMAT, fileProperties.ordinal(), fileProperties.generation());
    }

    private void recoverIndexFileMetas(Path indexFilePath) throws IOException {
        FileProperties fileProperties = indexFileProperties(indexFilePath);

        if (curFileOrdinal >= 0 && fileProperties.ordinal() != curFileOrdinal + 1) {
            throw new IllegalStateException(String.format(
                    "Unexpected index file ordinal. Expected %d, actual %d (%s).",
                    curFileOrdinal + 1, fileProperties.ordinal(), indexFilePath
            ));
        }

        curFileOrdinal = fileProperties.ordinal();

        try (InputStream is = new BufferedInputStream(Files.newInputStream(indexFilePath, StandardOpenOption.READ))) {
            ByteBuffer commonMetaBuffer = readBytes(is, COMMON_META_SIZE, indexFilePath);

            int magicNumber = commonMetaBuffer.getInt();

            if (magicNumber != MAGIC_NUMBER) {
                throw new IllegalStateException(String.format("Invalid magic number in index file %s: %d.", indexFilePath, magicNumber));
            }

            int formatVersion = commonMetaBuffer.getInt();

            if (formatVersion > FORMAT_VERSION) {
                throw new IllegalStateException(String.format(
                        "Unsupported format version in index file %s: %d.", indexFilePath, formatVersion
                ));
            }

            int numGroups = commonMetaBuffer.getInt();

            if (numGroups <= 0) {
                throw new IllegalStateException(String.format(
                        "Unexpected number of groups in index file %s: %d.",
                        indexFilePath, numGroups
                ));
            }

            for (int i = 0; i < numGroups; i++) {
                ByteBuffer groupMetaBuffer = readBytes(is, GROUP_META_SIZE, indexFilePath);

                long groupId = groupMetaBuffer.getLong();
                groupMetaBuffer.getInt(); // Skip flags.
                int payloadOffset = groupMetaBuffer.getInt();
                long firstLogIndexInclusive = groupMetaBuffer.getLong();
                long lastLogIndexExclusive = groupMetaBuffer.getLong();
                long firstIndexKept = groupMetaBuffer.getLong();

                IndexFileMeta indexFileMeta = createIndexFileMeta(
                        firstLogIndexInclusive, lastLogIndexExclusive, firstIndexKept, payloadOffset, fileProperties
                );

                var metaSpec = new IndexMetaSpec(groupId, indexFileMeta, firstIndexKept);

                putIndexFileMeta(metaSpec);
            }
        }
    }

    static FileProperties indexFileProperties(Path indexFile) {
        String fileName = indexFile.getFileName().toString();

        Matcher matcher = INDEX_FILE_NAME_PATTERN.matcher(fileName);

        if (!matcher.matches()) {
            throw new IllegalArgumentException(String.format("Invalid index file name format: %s.", indexFile));
        }

        return new FileProperties(
                Integer.parseInt(matcher.group("ordinal")),
                Integer.parseInt(matcher.group("generation"))
        );
    }

    private static ByteBuffer readBytes(InputStream is, int size, Path indexFile) throws IOException {
        ByteBuffer result = ByteBuffer.wrap(is.readNBytes(size)).order(BYTE_ORDER);

        if (result.remaining() != size) {
            throw new IOException(String.format("Unexpected EOF when trying to read from index file: %s.", indexFile));
        }

        return result;
    }

    private static class FileHeaderWithIndexMetas {
        private final byte[] header;

        private final List<IndexMetaSpec> indexMetas;

        FileHeaderWithIndexMetas(byte[] header, List<IndexMetaSpec> indexMetas) {
            this.header = header;
            this.indexMetas = indexMetas;
        }

        byte[] header() {
            return header;
        }

        List<IndexMetaSpec> indexMetas() {
            return indexMetas;
        }
    }

    private static class IndexMetaSpec {
        private final Long groupId;

        private final @Nullable IndexFileMeta indexFileMeta;

        private final long firstIndexKept;

        IndexMetaSpec(Long groupId, @Nullable IndexFileMeta indexFileMeta, long firstIndexKept) {
            this.groupId = groupId;
            this.indexFileMeta = indexFileMeta;
            this.firstIndexKept = firstIndexKept;
        }

        Long groupId() {
            return groupId;
        }

        @Nullable IndexFileMeta indexFileMeta() {
            return indexFileMeta;
        }

        long firstIndexKept() {
            return firstIndexKept;
        }
    }
}
