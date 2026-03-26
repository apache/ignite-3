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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class MostGarbageFirstCompactionStrategyTest extends AbstractCompactionStrategyTest {
    @Override
    protected SegmentFileCompactionStrategy createStrategy(Path segmentFilesDir, IndexFileManager indexFileManager) {
        return new MostGarbageFirstCompactionStrategy(segmentFilesDir, indexFileManager);
    }

    @Test
    void prioritizesFullyDeletableThenLeastLiveEntries() throws IOException {
        when(indexFileManager.describeSegmentFile(anyInt())).thenReturn(Long2ObjectMaps.emptyMap());

        FileProperties fullyDeletable = new FileProperties(3);
        FileProperties smallLiveSet = new FileProperties(2);
        FileProperties largerLiveSet = new FileProperties(1);
        FileProperties tieLowGeneration = new FileProperties(5, 0);
        FileProperties tieHighGeneration = new FileProperties(5, 1);

        createSegmentFile(fullyDeletable);
        createSegmentFile(smallLiveSet);
        createSegmentFile(largerLiveSet);
        createSegmentFile(tieLowGeneration);
        createSegmentFile(tieHighGeneration);

        createIndexFile(fullyDeletable);
        createIndexFile(smallLiveSet);
        createIndexFile(largerLiveSet);
        createIndexFile(tieLowGeneration);
        createIndexFile(tieHighGeneration);

        when(indexFileManager.describeSegmentFile(fullyDeletable.ordinal())).thenReturn(new Long2ObjectOpenHashMap<>());
        when(indexFileManager.describeSegmentFile(smallLiveSet.ordinal())).thenReturn(descriptor(meta(10, 11)));
        when(indexFileManager.describeSegmentFile(largerLiveSet.ordinal())).thenReturn(descriptor(meta(20, 23)));
        when(indexFileManager.describeSegmentFile(tieLowGeneration.ordinal())).thenReturn(descriptor(meta(30, 33)));

        assertThat(selectedCandidates(), contains(fullyDeletable, smallLiveSet, largerLiveSet, tieLowGeneration, tieHighGeneration));
    }

    private static Long2ObjectMap<IndexFileMeta> descriptor(IndexFileMeta... metas) {
        var map = new Long2ObjectOpenHashMap<IndexFileMeta>();

        for (int i = 0; i < metas.length; i++) {
            map.put(i, metas[i]);
        }

        return map;
    }

    private static IndexFileMeta meta(long firstLogIndexInclusive, long lastLogIndexExclusive) {
        return new IndexFileMeta(firstLogIndexInclusive, lastLogIndexExclusive, 0, new FileProperties(0));
    }
}
