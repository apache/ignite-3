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
    public CompletableFuture<Void> onSnapshotSave(
            long lastAppliedIndex,
            long lastAppliedTerm,
            Collection<RaftTableProcessor> tableProcessors
    ) {
        tableProcessors.forEach(processor -> processor.lastApplied(lastAppliedIndex, lastAppliedTerm));

        txStatePartitionStorage.lastApplied(lastAppliedIndex, lastAppliedTerm);

        updateTrackerIgnoringTrackerClosedException(storageIndexTracker, lastAppliedIndex);

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
