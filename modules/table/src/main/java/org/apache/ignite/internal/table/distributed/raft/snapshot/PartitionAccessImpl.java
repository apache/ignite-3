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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * {@link PartitionAccess} that adapts an {@link MvPartitionStorage}.
 */
public class PartitionAccessImpl implements PartitionAccess {
    private final PartitionKey partitionKey;

    private final MvPartitionStorage partitionStorage;

    private final TxStateStorage txStateStorage;

    /**
     * Creates a new instance.
     */
    public PartitionAccessImpl(PartitionKey partitionKey, MvPartitionStorage partitionStorage, TxStateStorage txStateStorage) {
        this.partitionKey = partitionKey;
        this.partitionStorage = partitionStorage;
        this.txStateStorage = txStateStorage;
    }

    @Override
    public PartitionKey key() {
        return partitionKey;
    }

    @Override
    public long persistedIndex() {
        return partitionStorage.persistedIndex();
    }

    @Override
    public @Nullable RowId closestRowId(RowId lowerBound) {
        return partitionStorage.closestRowId(lowerBound);
    }

    @Override
    public List<ReadResult> rowVersions(RowId rowId) {
        try (Cursor<ReadResult> cursor = partitionStorage.scanVersions(rowId)) {
            List<ReadResult> versions = new ArrayList<>();

            for (ReadResult version : cursor) {
                versions.add(version);
            }

            return versions;
        } catch (Exception e) {
            // TODO: IGNITE-17935 - handle this

            throw new RuntimeException(e);
        }
    }

    @Override
    public Cursor<IgniteBiTuple<UUID, TxMeta>> scanTxData() {
        return txStateStorage.scan();
    }
}
