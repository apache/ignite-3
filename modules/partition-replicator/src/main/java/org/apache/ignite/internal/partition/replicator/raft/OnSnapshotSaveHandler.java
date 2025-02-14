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

package org.apache.ignite.internal.partition.replicator.raft;

import static java.lang.Math.max;
import static java.util.concurrent.CompletableFuture.allOf;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.util.TrackerClosedException;

/**
 * Handler for the {@link RaftGroupListener#onSnapshotSave} event.
 */
public class OnSnapshotSaveHandler {
    private final TxStatePartitionStorage txStatePartitionStorage;

    private final PendingComparableValuesTracker<Long, Void> storageIndexTracker;

    public OnSnapshotSaveHandler(
            TxStatePartitionStorage txStatePartitionStorage,
            PendingComparableValuesTracker<Long, Void> storageIndexTracker
    ) {
        this.txStatePartitionStorage = txStatePartitionStorage;
        this.storageIndexTracker = storageIndexTracker;
    }

    /**
     * Called when {@link RaftGroupListener#onSnapshotSave} is triggered.
     */
    public CompletableFuture<Void> onSnapshotSave(Collection<RaftTableProcessor> tableProcessors) {
        // The max index here is required for local recovery and a possible scenario
        // of false node failure when we actually have all required data. This might happen because we use the minimal index
        // among storages on a node restart.
        // Let's consider a more detailed example:
        //      1) We don't propagate the maximal lastAppliedIndex among storages, and onSnapshotSave finishes, it leads to the raft log
        //         truncation until the maximal lastAppliedIndex.
        //      2) Unexpected cluster restart happens.
        //      3) Local recovery of a node is started, where we request data from the minimal lastAppliedIndex among storages, because
        //         some data for some node might not have been flushed before unexpected cluster restart.
        //      4) When we try to restore data starting from the minimal lastAppliedIndex, we come to the situation
        //         that a raft node doesn't have such data, because the truncation until the maximal lastAppliedIndex from 1) has happened.
        //      5) Node cannot finish local recovery.

        long maxPartitionLastAppliedIndex = tableProcessors.stream()
                .mapToLong(RaftTableProcessor::lastAppliedIndex)
                .max()
                .orElse(0);

        long maxPartitionLastAppliedTerm = tableProcessors.stream()
                .mapToLong(RaftTableProcessor::lastAppliedTerm)
                .max()
                .orElse(0);

        long maxLastAppliedIndex = max(maxPartitionLastAppliedIndex, txStatePartitionStorage.lastAppliedIndex());

        long maxLastAppliedTerm = max(maxPartitionLastAppliedTerm, txStatePartitionStorage.lastAppliedTerm());

        tableProcessors.forEach(processor -> processor.lastApplied(maxLastAppliedIndex, maxLastAppliedTerm));

        txStatePartitionStorage.lastApplied(maxLastAppliedIndex, maxLastAppliedTerm);

        updateTrackerIgnoringTrackerClosedException(storageIndexTracker, maxLastAppliedIndex);

        Stream<CompletableFuture<?>> flushFutures = Stream.concat(
                tableProcessors.stream().map(RaftTableProcessor::flushStorage),
                Stream.of(txStatePartitionStorage.flush())
        );

        return allOf(flushFutures.toArray(CompletableFuture[]::new));
    }

    private static <T extends Comparable<T>> void updateTrackerIgnoringTrackerClosedException(
            PendingComparableValuesTracker<T, Void> tracker,
            T newValue
    ) {
        try {
            tracker.update(newValue, null);
        } catch (TrackerClosedException ignored) {
            // No-op.
        }
    }
}
