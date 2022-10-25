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
import java.util.concurrent.Executor;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;

/**
 * {@link PartitionAccess} implementation.
 */
public class PartitionAccessImpl implements PartitionAccess {
    private final MvTableStorage mvTableStorage;

    private final TxStateTableStorage txStateTableStorage;

    private final int partId;

    /**
     * Constructor.
     *
     * @param mvTableStorage Multi version table storage.
     * @param txStateTableStorage Table transaction state storage.
     * @param partId Partition ID.
     */
    public PartitionAccessImpl(
            MvTableStorage mvTableStorage,
            TxStateTableStorage txStateTableStorage,
            int partId
    ) {
        this.mvTableStorage = mvTableStorage;
        this.txStateTableStorage = txStateTableStorage;
        this.partId = partId;
    }

    @Override
    public int partitionId() {
        return partId;
    }

    @Override
    public MvPartitionStorage mvPartitionStorage() {
        MvPartitionStorage mvPartition = mvTableStorage.getMvPartition(partId);

        assert mvPartition != null : "table=" + tableName() + ", part=" + partId;

        return mvPartition;
    }

    @Override
    public TxStateStorage txStatePartitionStorage() {
        TxStateStorage txStatePartitionStorage = txStateTableStorage.getOrCreateTxStateStorage(partId);

        assert txStatePartitionStorage != null : "table=" + tableName() + ", part=" + partId;

        return txStatePartitionStorage;
    }

    @Override
    public CompletableFuture<MvPartitionStorage> reCreateMvPartitionStorage(Executor executor) throws StorageException {
        assert mvTableStorage.getMvPartition(partId) != null : "table=" + tableName() + ", part=" + partId;

        return mvTableStorage
                .destroyPartition(partId)
                .thenApplyAsync(unused -> mvTableStorage.getOrCreateMvPartition(partId), executor);
    }

    @Override
    public CompletableFuture<TxStateStorage> reCreateTxStatePartitionStorage(Executor executor) throws StorageException {
        assert txStateTableStorage.getTxStateStorage(partId) != null : "table=" + tableName() + ", part=" + partId;

        return txStateTableStorage
                .destroyTxStateStorage(partId)
                .thenApplyAsync(unused -> txStateTableStorage.getOrCreateTxStateStorage(partId), executor);
    }

    private String tableName() {
        return mvTableStorage.configuration().name().value();
    }
}
