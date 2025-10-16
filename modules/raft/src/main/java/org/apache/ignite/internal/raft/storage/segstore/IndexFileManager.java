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

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.util.IgniteUtils.atomicMoveFile;
import static org.apache.ignite.internal.util.IgniteUtils.fsyncFile;

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
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
 * <p>Raft group meta is as follows:
 * <pre>
 * +----------------------------------------------------------------------------------------------------------------+-----+
 * |                                              Raft group 1 meta                                                 | ... |
 * +----------------------------------------------------------------------------------------------------------------+-----+
 * | Group ID (8 bytes) | Flags (4 bytes) | Offset (4 bytes) | First Log Index (8 bytes) | Last Log Index (8 bytes) | ... |
 * +----------------------------------------------------------------------------------------------------------------+-----+
 * </pre>
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
    private static final IgniteLogger LOG = Loggers.forClass(IndexFileManager.class);

    static final int MAGIC_NUMBER = 0x6BF0A76A;

    static final int FORMAT_VERSION = 1;

    private static final String INDEX_FILE_NAME_FORMAT = "index-%010d-%010d.bin";

    // Magic number + format version + number of Raft groups.
    static final int COMMON_META_SIZE = Integer.BYTES + Integer.BYTES + Integer.BYTES;

    // Group ID + flags + file offset + start log index + end log index.
    static final int GROUP_META_SIZE = Long.BYTES + Integer.BYTES + Integer.BYTES + Long.BYTES + Long.BYTES;

    static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    private final Path baseDir;

    /**
     * Current index file ordinal (used to generate index file names).
     *
     * <p>No synchronized access is needed because this field is only used by the checkpoint thread.
     */
    private int curFileOrdinal = 0;

    /**
     * Index file metadata grouped by Raft Group ID.
     */
    private final Map<Long, GroupIndexMeta> groupIndexMetas = new ConcurrentHashMap<>();

    IndexFileManager(Path baseDir) {
        this.baseDir = baseDir;
    }

    /**
     * Saves the given index memtable to a file.
     */
    Path saveIndexMemtable(ReadModeIndexMemTable indexMemTable) throws IOException {
        String fileName = indexFileName(curFileOrdinal, 0);

        Path tmpFilePath = baseDir.resolve(fileName + ".tmp");

        try (var os = new BufferedOutputStream(Files.newOutputStream(tmpFilePath, CREATE_NEW, WRITE))) {
            byte[] headerBytes = serializeHeaderAndFillMetadata(indexMemTable);

            os.write(headerBytes);

            Iterator<Entry<Long, SegmentInfo>> it = indexMemTable.iterator();

            while (it.hasNext()) {
                os.write(payload(it.next().getValue()));
            }
        }

        curFileOrdinal++;

        return syncAndRename(tmpFilePath, tmpFilePath.resolveSibling(fileName));
    }

    /**
     * Returns a pointer into a segment file that contains the entry for the given group's index. Returns {@code null} if the given log
     * index could not be found in any of the index files.
     */
    @Nullable
    SegmentFilePointer getSegmentFilePointer(long groupId, long logIndex) throws IOException {
        GroupIndexMeta groupIndexMeta = groupIndexMetas.get(groupId);

        if (groupIndexMeta == null) {
            return null;
        }

        IndexFileMeta indexFileMeta = groupIndexMeta.indexMeta(logIndex);

        if (indexFileMeta == null) {
            return null;
        }

        Path indexFile = baseDir.resolve(indexFileName(indexFileMeta.indexFileOrdinal(), 0));

        // Index file payload is a 0-based array, which indices correspond to the [fileMeta.firstLogIndex, fileMeta.lastLogIndex] range.
        long payloadArrayIndex = logIndex - indexFileMeta.firstLogIndexInclusive();

        assert payloadArrayIndex >= 0 : payloadArrayIndex;

        long payloadOffset = indexFileMeta.indexFilePayloadOffset() + payloadArrayIndex * Integer.BYTES;

        try (SeekableByteChannel channel = Files.newByteChannel(indexFile, StandardOpenOption.READ)) {
            channel.position(payloadOffset);

            ByteBuffer segmentPayloadOffsetBuffer = ByteBuffer.allocate(Integer.BYTES).order(BYTE_ORDER);

            while (segmentPayloadOffsetBuffer.hasRemaining()) {
                int bytesRead = channel.read(segmentPayloadOffsetBuffer);

                if (bytesRead == -1) {
                    throw new EOFException("EOF reached while reading index file: " + indexFile);
                }
            }

            int segmentPayloadOffset = segmentPayloadOffsetBuffer.getInt(0);

            return new SegmentFilePointer(indexFileMeta.indexFileOrdinal(), segmentPayloadOffset);
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
     * Returns the highest log index for the given group across all index files or {@code -1} if no such index exists.
     */
    long lastLogIndexExclusive(long groupId) {
        GroupIndexMeta groupIndexMeta = groupIndexMetas.get(groupId);

        return groupIndexMeta == null ? -1 : groupIndexMeta.lastLogIndexExclusive();
    }

    private byte[] serializeHeaderAndFillMetadata(ReadModeIndexMemTable indexMemTable) {
        int numGroups = indexMemTable.numGroups();

        int headerSize = headerSize(numGroups);

        ByteBuffer headerBuffer = ByteBuffer.allocate(headerSize)
                .order(BYTE_ORDER)
                .putInt(MAGIC_NUMBER)
                .putInt(FORMAT_VERSION)
                .putInt(numGroups);

        int payloadOffset = headerSize;

        Iterator<Entry<Long, SegmentInfo>> it = indexMemTable.iterator();

        while (it.hasNext()) {
            Entry<Long, SegmentInfo> entry = it.next();

            // Using the boxed value to avoid unnecessary autoboxing later.
            Long groupId = entry.getKey();

            SegmentInfo segmentInfo = entry.getValue();

            long firstLogIndexInclusive = segmentInfo.firstLogIndexInclusive();

            long lastLogIndexExclusive = segmentInfo.lastLogIndexExclusive();

            var indexFileMeta = new IndexFileMeta(firstLogIndexInclusive, lastLogIndexExclusive, payloadOffset, curFileOrdinal);

            putIndexFileMeta(groupId, indexFileMeta);

            headerBuffer
                    .putLong(groupId)
                    .putInt(0) // Flags.
                    .putInt(payloadOffset)
                    .putLong(firstLogIndexInclusive)
                    .putLong(lastLogIndexExclusive);

            payloadOffset += payloadSize(segmentInfo);
        }

        return headerBuffer.array();
    }

    private void putIndexFileMeta(Long groupId, IndexFileMeta indexFileMeta) {
        GroupIndexMeta existingGroupIndexMeta = groupIndexMetas.get(groupId);

        if (existingGroupIndexMeta == null) {
            groupIndexMetas.put(groupId, new GroupIndexMeta(indexFileMeta));
        } else {
            existingGroupIndexMeta.addIndexMeta(indexFileMeta);
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

    private static String indexFileName(int fileOrdinal, int generation) {
        return String.format(INDEX_FILE_NAME_FORMAT, fileOrdinal, generation);
    }
}
