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

package org.apache.ignite.internal.tx.storage.state.rocksdb;

import static java.util.Collections.reverse;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorageException;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;

/**
 * RocksDb implementation of {@link TxStateStorage}.
 */
public class TxStateRocksDbStorage implements TxStateStorage {
    static final int TABLE_OR_ZONE_ID_SIZE_BYTES = Integer.BYTES;

    /** Prefix length for the payload within a table/zone. Consists of tableId/zoneId (4 bytes) in Big Endian.  */
    static final int TABLE_OR_ZONE_PREFIX_SIZE_BYTES = TABLE_OR_ZONE_ID_SIZE_BYTES;

    /** Partition storages. */
    private final AtomicReferenceArray<TxStateRocksDbPartitionStorage> storages;

    /** Prevents double stopping the storage. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Table/zone ID. */
    final int id;

    final TxStateRocksDbSharedStorage sharedStorage;

    /**
     * Constructor.
     *
     * @param id Table/zone ID.
     * @param partitions Count of partitions.
     */
    public TxStateRocksDbStorage(
            int id,
            int partitions,
            TxStateRocksDbSharedStorage sharedStorage
    ) {
        this.id = id;

        this.storages = new AtomicReferenceArray<>(partitions);
        this.sharedStorage = sharedStorage;
    }

    /**
     * Checks that a passed partition id is within the proper bounds.
     *
     * @param partitionId Partition id.
     */
    private void checkPartitionId(int partitionId) {
        if (partitionId < 0 || partitionId >= storages.length()) {
            throw new IllegalArgumentException(S.toString(
                    "Unable to access partition with id outside of configured range",
                    "tableId/zoneId", id, false,
                    "partitionId", partitionId, false,
                    "partitions", storages.length(), false
            ));
        }
    }

    @Override
    public TxStateRocksDbPartitionStorage getOrCreatePartitionStorage(int partitionId) {
        checkPartitionId(partitionId);

        TxStateRocksDbPartitionStorage storage = storages.get(partitionId);

        if (storage == null) {
            storage = createPartitionStorage(partitionId);

            storage.start();
        }

        storages.set(partitionId, storage);

        return storage;
    }

    /*
     * Creates transaction state storage for partition.
     *
     * @param partitionId Partition id.
     * @throws IgniteInternalException In case when the operation has failed.
     */
    protected TxStateRocksDbPartitionStorage createPartitionStorage(int partitionId) {
        return new TxStateRocksDbPartitionStorage(partitionId, this);
    }

    @Override
    public @Nullable TxStatePartitionStorage getPartitionStorage(int partitionId) {
        return storages.get(partitionId);
    }

    @Override
    public void destroyPartitionStorage(int partitionId) {
        checkPartitionId(partitionId);

        TxStatePartitionStorage storage = storages.getAndSet(partitionId, null);

        if (storage != null) {
            storage.destroy();
        }
    }

    @Override
    public void start() {
    }

    @Override
    public void close() {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        try {
            List<AutoCloseable> resources = new ArrayList<>();

            for (int i = 0; i < storages.length(); i++) {
                TxStatePartitionStorage storage = storages.get(i);

                if (storage != null) {
                    resources.add(storage::close);
                }
            }

            reverse(resources);
            closeAll(resources);
        } catch (Exception e) {
            throw new TxStateStorageException("Failed to stop transaction state storage [tableOrZoneId={}]", e, id);
        }
    }

    @Override
    public void destroy() {
        close();
        sharedStorage.destroyStorage(id);
    }
}
