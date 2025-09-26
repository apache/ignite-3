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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map.Entry;

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
 * @see ReadModeIndexMemTable
 * @see SegmentFileManager
 */
class IndexFileManager {
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
     * Current index file index (used to generate index file names).
     *
     * <p>No synchronized access is needed because this field is only used by the checkpoint thread.
     */
    private int curFileIndex = 0;

    IndexFileManager(Path baseDir) {
        this.baseDir = baseDir;
    }

    /**
     * Saves the given index memtable to a file.
     *
     * <p>The file is saved into a temporary location and is expected to be later renamed using {@link IndexFile#syncAndRename}.
     */
    IndexFile saveIndexMemtable(ReadModeIndexMemTable indexMemTable) throws IOException {
        String fileName = indexFileName(curFileIndex++, 0);

        Path path = baseDir.resolve(fileName + ".tmp");

        try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(path, CREATE_NEW, WRITE))) {
            os.write(header(indexMemTable));

            Iterator<Entry<Long, SegmentInfo>> it = indexMemTable.iterator();

            while (it.hasNext()) {
                os.write(payload(it.next().getValue()));
            }
        }

        return new IndexFile(fileName, path);
    }

    private static byte[] header(ReadModeIndexMemTable indexMemTable) {
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

            long groupId = entry.getKey();

            SegmentInfo segmentInfo = entry.getValue();

            headerBuffer
                    .putLong(groupId)
                    .putInt(0) // Flags.
                    .putInt(payloadOffset)
                    .putLong(segmentInfo.firstLogIndex())
                    .putLong(segmentInfo.lastLogIndex());

            payloadOffset += payloadSize(segmentInfo);
        }

        return headerBuffer.array();
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

    private static String indexFileName(int fileIndex, int generation) {
        return String.format(INDEX_FILE_NAME_FORMAT, fileIndex, generation);
    }
}
