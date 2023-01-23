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

package org.apache.ignite.internal.table.distributed.raft.snapshot;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RaftGroupConfiguration;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;

/**
 * Small abstractions for partition storages that includes only methods, mandatory for the snapshot storage.
 */
public interface PartitionAccess {
    /**
     * Returns the key that uniquely identifies the corresponding partition.
     */
    PartitionKey partitionKey();

    /**
     * Returns the multi-versioned partition storage.
     */
    MvPartitionStorage mvPartitionStorage();

    /**
     * Returns transaction state storage for the partition.
     */
    TxStateStorage txStatePartitionStorage();

    /**
     * Prepares partition storages for rebalancing.
     * <ul>
     *     <li>Cancels all current operations (including cursors) with storages and waits for their completion;</li>
     *     <li>Cleans up storages;</li>
     *     <li>Sets the last applied index and term to {@link MvPartitionStorage#REBALANCE_IN_PROGRESS} and the RAFT group configuration to
     *     {@code null};</li>
     *     <li>Only the following methods will be available:<ul>
 *         <li>TODO-18593 Specify methods.</li>
     *     </ul></li>
     * </ul>
     *
     * <p>This method must be called before every rebalance and ends with a call to one of the methods:
     * <ul>
     *     <li>{@link #abortRebalance()} - in case of errors or cancellation of rebalance;</li>
     *     <li>{@link #finishRebalance(long, long, RaftGroupConfiguration)} - in case of successful completion of rebalance.</li>
     * </ul>
     *
     * @return Future of the operation.
     * @throws StorageRebalanceException If there are errors when trying to start rebalancing.
     */
    CompletableFuture<Void> startRebalance();

    /**
     * Aborts rebalancing of the partition storages.
     * <ul>
     *     <li>Cleans up storages;</li>
     *     <li>Resets the last applied index, term, and RAFT group configuration;</li>
     *     <li>All methods will be available.</li>
     * </ul>
     *
     * <p>If rebalance has not started, then nothing will happen.
     *
     * @return Future of the operation.
     * @throws StorageRebalanceException If there are errors when trying to abort rebalancing.
     */
    CompletableFuture<Void> abortRebalance();

    /**
     * Completes rebalancing of the partition storages.
     * <ul>
     *     <li>Cleans up storages;</li>
     *     <li>Updates the last applied index, term, and RAFT group configuration;</li>
     *     <li>All methods will be available.</li>
     * </ul>
     *
     * <p>If rebalance has not started, then {@link StorageRebalanceException} will be thrown.
     *
     * @param lastAppliedIndex Last applied index.
     * @param lastAppliedTerm Last applied term.
     * @param raftGroupConfig RAFT group configuration.
     * @return Future of the operation.
     * @throws StorageRebalanceException If there are errors when trying to finish rebalancing.
     */
    CompletableFuture<Void> finishRebalance(long lastAppliedIndex, long lastAppliedTerm, RaftGroupConfiguration raftGroupConfig);
}
