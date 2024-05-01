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

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.util.Collections.reverse;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;

/**
 * RocksDb implementation of {@link TxStateTableStorage}.
 */
public class TxStateRocksDbTableStorage implements TxStateTableStorage {
    /** Prefix length for the payload within a table. Consists of tableId (4 bytes) in Big Endian.  */
    static final int TABLE_PREFIX_SIZE_BYTES = Integer.BYTES;

    /** Partition storages. */
    private final AtomicReferenceArray<TxStateRocksDbStorage> storages;

    /** Prevents double stopping the storage. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Table ID. */
    final int id;

    final TxStateRocksDbSharedStorage sharedStorage;

    /**
     * Constructor.
     *
     * @param id Table ID.
     * @param partitions Count of partitions.
     */
    public TxStateRocksDbTableStorage(
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
                "tableId", id, false,
                "partitionId", partitionId, false,
                "partitions", storages.length(), false
            ));
        }
    }

    @Override
    public TxStateStorage getOrCreateTxStateStorage(int partitionId) {
        checkPartitionId(partitionId);

        TxStateRocksDbStorage storage = storages.get(partitionId);

        if (storage == null) {
            storage = new TxStateRocksDbStorage(
                partitionId,
                this
            );

            storage.start();
        }

        storages.set(partitionId, storage);

        return storage;
    }

    @Override
    public @Nullable TxStateStorage getTxStateStorage(int partitionId) {
        return storages.get(partitionId);
    }

    @Override
    public void destroyTxStateStorage(int partitionId) {
        checkPartitionId(partitionId);

        TxStateStorage storage = storages.getAndSet(partitionId, null);

        if (storage != null) {
            storage.destroy();
        }
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        try {
            List<AutoCloseable> resources = new ArrayList<>();

            for (int i = 0; i < storages.length(); i++) {
                TxStateStorage storage = storages.get(i);

                if (storage != null) {
                    resources.add(storage::close);
                }
            }

            reverse(resources);
            closeAll(resources);
        } catch (Exception e) {
            throw new IgniteInternalException("Failed to stop transaction state storage of the table: " + id, e);
        }
    }

    @Override
    public void destroy() {
        byte[] start = ByteBuffer.allocate(TABLE_PREFIX_SIZE_BYTES).order(BIG_ENDIAN).putInt(id).array();
        byte[] end = ByteBuffer.allocate(TABLE_PREFIX_SIZE_BYTES).order(BIG_ENDIAN).putInt(id + 1).array();

        try {
            close();

            sharedStorage.db().deleteRange(start, end);
        } catch (Exception e) {
            throw new IgniteInternalException("Failed to destroy the transaction state storage of the table: " + id, e);
        }
    }

    @Override
    public void close() {
        stop();
    }
}
