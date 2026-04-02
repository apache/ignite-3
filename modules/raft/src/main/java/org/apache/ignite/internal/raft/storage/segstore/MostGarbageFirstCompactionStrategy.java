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

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * Compaction strategy that prioritizes files with the most dead entries, fully-deletable files first.
 */
class MostGarbageFirstCompactionStrategy implements SegmentFileCompactionStrategy {
    private final Path segmentFilesDir;

    private final IndexFileManager indexFileManager;

    MostGarbageFirstCompactionStrategy(Path segmentFilesDir, IndexFileManager indexFileManager) {
        this.segmentFilesDir = segmentFilesDir;
        this.indexFileManager = indexFileManager;
    }

    @Override
    public Stream<FileProperties> selectCandidates() throws IOException {
        var scores = new Object2LongOpenHashMap<FileProperties>();

        Comparator<FileProperties> comparator =
                Comparator.<FileProperties>comparingLong(props -> scores.computeIfAbsent(props, this::score))
                        .thenComparing(Comparator.naturalOrder());

        //noinspection resource
        return Files.list(segmentFilesDir)
                .filter(p -> !p.getFileName().toString().endsWith(".tmp"))
                .map(SegmentFile::fileProperties)
                .filter(props -> Files.exists(indexFileManager.indexFilePath(props)))
                .sorted(comparator);
    }

    private long score(FileProperties props) {
        Long2ObjectMap<IndexFileMeta> description = indexFileManager.describeSegmentFile(props.ordinal());

        if (description.isEmpty()) {
            return -1; // Fully deletable — highest priority.
        }

        long liveCount = 0;

        for (IndexFileMeta meta : description.values()) {
            liveCount += meta.lastLogIndexExclusive() - meta.firstLogIndexInclusive();
        }

        return liveCount;
    }
}
