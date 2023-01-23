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
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.lang.IgniteStringFormatter;

/**
 * {@link PartitionAccess} implementation.
 */
public class PartitionAccessImpl implements PartitionAccess {
    private final PartitionKey partitionKey;

    private final MvTableStorage mvTableStorage;

    private final TxStateTableStorage txStateTableStorage;

    /**
     * Constructor.
     *
     * @param partitionKey Partition key.
     * @param mvTableStorage Multi version table storage.
     * @param txStateTableStorage Table transaction state storage.
     */
    public PartitionAccessImpl(
            PartitionKey partitionKey,
            MvTableStorage mvTableStorage,
            TxStateTableStorage txStateTableStorage
    ) {
        this.partitionKey = partitionKey;
        this.mvTableStorage = mvTableStorage;
        this.txStateTableStorage = txStateTableStorage;
    }

    @Override
    public PartitionKey partitionKey() {
        return partitionKey;
    }

    @Override
    public MvPartitionStorage mvPartitionStorage() {
        MvPartitionStorage mvPartition = mvTableStorage.getMvPartition(partId());

        assert mvPartition != null : "table=" + tableName() + ", part=" + partId();

        return mvPartition;
    }

    @Override
    public TxStateStorage txStatePartitionStorage() {
        return getTxStateStorage(partId());
    }

    @Override
    public CompletableFuture<Void> startRebalance() {
        TxStateStorage txStateStorage = getTxStateStorage(partId());

        return CompletableFuture.allOf(
                mvTableStorage.startRebalancePartition(partId()),
                txStateStorage.startRebalance()
        );
    }

    @Override
    public CompletableFuture<Void> abortRebalance() {
        TxStateStorage txStateStorage = getTxStateStorage(partId());

        return CompletableFuture.allOf(
                mvTableStorage.abortRebalancePartition(partId()),
                txStateStorage.abortRebalance()
        );
    }

    @Override
    public CompletableFuture<Void> finishRebalance(long lastAppliedIndex, long lastAppliedTerm, RaftGroupConfiguration raftGroupConfig) {
        TxStateStorage txStateStorage = getTxStateStorage(partId());

        return CompletableFuture.allOf(
                mvTableStorage.finishRebalancePartition(partId(), lastAppliedIndex, lastAppliedTerm, raftGroupConfig),
                txStateStorage.finishRebalance(lastAppliedIndex, lastAppliedTerm)
        );
    }

    private int partId() {
        return partitionKey.partitionId();
    }

    private String tableName() {
        return mvTableStorage.configuration().name().value();
    }

    private TxStateStorage getTxStateStorage(int partitionId) {
        TxStateStorage txStateStorage = txStateTableStorage.getTxStateStorage(partitionId);

        assert txStateStorage != null : IgniteStringFormatter.format("table={}, partitionId={}", tableName(), partitionId);

        return txStateStorage;
    }
}
