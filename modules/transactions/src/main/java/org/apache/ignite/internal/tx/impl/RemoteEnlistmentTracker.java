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

package org.apache.ignite.internal.tx.impl;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Tracks remote transaction enlistments on partition owner nodes.
 * Used to clean up abandoned write intents when client connections are lost.
 *
 * <p>This tracker is used in the lightweight client transaction coordinator mode
 * where clients can send operations directly to partition owners (TX_ID_DIRECT).
 * When a client connection is lost, the coordinator node can use this tracker
 * to identify and clean up all remote enlistments created by that transaction.
 */
public class RemoteEnlistmentTracker {
    /**
     * Map: Transaction ID -> Set of enlisted (tableId, partitionId) pairs.
     * This tracks all remote enlistments on this node.
     */
    private final ConcurrentHashMap<UUID, Set<IgniteBiTuple<Integer, Integer>>> remoteEnlistments = new ConcurrentHashMap<>();

    /**
     * Records that a remote transaction enlisted a partition on this node.
     *
     * @param txId Transaction ID.
     * @param tableId Table ID.
     * @param partitionId Partition ID.
     */
    public void trackEnlistment(UUID txId, int tableId, int partitionId) {
        remoteEnlistments.computeIfAbsent(txId, k -> ConcurrentHashMap.newKeySet())
                .add(new IgniteBiTuple<>(tableId, partitionId));
    }

    /**
     * Gets all tracked enlistments for a transaction.
     *
     * @param txId Transaction ID.
     * @return Set of (tableId, partitionId) pairs, or null if no enlistments tracked.
     */
    @Nullable
    public Set<IgniteBiTuple<Integer, Integer>> getEnlistments(UUID txId) {
        return remoteEnlistments.get(txId);
    }

    /**
     * Removes tracking for a transaction (after cleanup).
     *
     * @param txId Transaction ID.
     */
    public void removeTracking(UUID txId) {
        remoteEnlistments.remove(txId);
    }

    /**
     * Returns the number of tracked transactions.
     * Used for testing and metrics.
     *
     * @return Number of tracked transactions.
     */
    public int trackedTransactionsCount() {
        return remoteEnlistments.size();
    }
}
