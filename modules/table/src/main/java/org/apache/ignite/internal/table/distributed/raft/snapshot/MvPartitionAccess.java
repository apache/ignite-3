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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.jetbrains.annotations.Nullable;

/**
 * {@link MvPartitionStorage} based implementation.
 */
public class MvPartitionAccess implements PartitionAccess {
    private final MvTableStorage tableStorage;

    private final int partId;

    /**
     * Constructor.
     *
     * @param tableStorage Table storage.
     * @param partId Partition ID.
     */
    public MvPartitionAccess(MvTableStorage tableStorage, int partId) {
        this.tableStorage = tableStorage;
        this.partId = partId;
    }

    @Override
    public long persistedIndex() {
        MvPartitionStorage mvPartition = tableStorage.getMvPartition(partId);

        assert mvPartition != null;

        return mvPartition.persistedIndex();
    }

    @Override
    public CompletableFuture<Void> reCreatePartition() throws StorageException {
        return tableStorage.destroyPartition(partId)
                .thenAccept(unused -> {
                    MvPartitionStorage partition = tableStorage.getOrCreateMvPartition(partId);

                    assert partition.persistedIndex() == 0 : "table=" + tableStorage.configuration().name().key() + ", part=" + partId;
                    assert partition.lastAppliedIndex() == 0 : "table=" + tableStorage.configuration().name().key() + ", part=" + partId;
                });
    }

    @Override
    public void lastAppliedIndex(long lastAppliedIndex) throws StorageException {
        MvPartitionStorage mvPartition = tableStorage.getMvPartition(partId);

        assert mvPartition != null;

        mvPartition.runConsistently(() -> {
            mvPartition.lastAppliedIndex(lastAppliedIndex);

            return null;
        });
    }

    @Override
    public void writeVersionChain(
            UUID rowId,
            List<ByteBuffer> rowVersions,
            List<HybridTimestamp> timestamps,
            @Nullable UUID txId,
            @Nullable UUID commitTableId,
            int commitPartitionId
    ) throws StorageException {
        MvPartitionStorage mvPartition = tableStorage.getMvPartition(partId);

        assert mvPartition != null;

        mvPartition.runConsistently(() -> {
            // TODO: IGNITE-17894 реализовать

            return null;
        });
    }
}
