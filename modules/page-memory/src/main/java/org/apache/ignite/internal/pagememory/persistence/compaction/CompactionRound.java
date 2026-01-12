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

package org.apache.ignite.internal.pagememory.persistence.compaction;

import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.ignite.internal.pagememory.persistence.store.DeltaFilePageStoreIo;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;

/** Data class for compaction round. */
class CompactionRound {
    /** Compaction round ID. */
    final UUID id = UUID.randomUUID();

    /** Total number of partition files. */
    final int partitionFileCount;

    /** Total number of all partition delta files. */
    final int totalDeltaFileCount;

    /** Queue of delta files (one per partition) to be merged into the partition file. */
    final Queue<DeltaFileForCompaction> queue;

    private CompactionRound(int partitionFileCount, int totalDeltaFileCount, Queue<DeltaFileForCompaction> queue) {
        this.partitionFileCount = partitionFileCount;
        this.totalDeltaFileCount = totalDeltaFileCount;
        this.queue = queue;
    }

    static CompactionRound create(FilePageStoreManager filePageStoreManager) {
        var partitionFileCount = new int[]{0};
        var totalDeltaFileCount = new int[]{0};

        var queue = new ConcurrentLinkedQueue<DeltaFileForCompaction>();

        filePageStoreManager.allPageStores().forEach(pageStore -> {
            partitionFileCount[0]++;

            totalDeltaFileCount[0] += pageStore.pageStore().deltaFileCount();

            DeltaFilePageStoreIo deltaFileToCompaction = pageStore.pageStore().getDeltaFileToCompaction();

            if (deltaFileToCompaction != null) {
                queue.add(new DeltaFileForCompaction(pageStore, deltaFileToCompaction));
            }
        });

        return new CompactionRound(partitionFileCount[0], totalDeltaFileCount[0], queue);
    }
}
