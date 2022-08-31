/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.tx.storage.state;

import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.NotNull;

/**
 * Storage for transaction meta, {@link TxMeta}.
 */
public interface TxStateStorage extends AutoCloseable {
    /**
     * Start the storage.
     *
     * @throws IgniteInternalException with {@link Transactions#TX_STATE_STORAGE_CREATE_ERR} error code in case when
     *                                 creation of the storage has failed.
     */
    void start();

    /**
     * Whether the storage is started.
     *
     * @return {@code true} if the storage is started, {@code false} otherwise.
     */
    boolean isStarted();

    /**
     * Stop the storage.
     */
    void stop() throws Exception;

    /**
     * Get tx meta by tx id.
     *
     * @param txId Tx id.
     * @return Tx meta.
     * @throws IgniteInternalException with {@link Transactions#TX_STATE_STORAGE_ERR} error code in case when
     *                                 the operation has failed.
     */
    TxMeta get(UUID txId);

    /**
     * Put the tx meta into the storage.
     *
     * @param txId Tx id.
     * @param txMeta Tx meta.
     * @throws IgniteInternalException with {@link Transactions#TX_STATE_STORAGE_ERR} error code in case when
     *                                 the operation has failed.
     */
    void put(UUID txId, TxMeta txMeta);

    /**
     * Atomically change the tx meta in the storage.
     *
     * @param txId Tx id.
     * @param txStateExpected Tx state that is expected to be in the storage.
     * @param txMeta Tx meta.
     * @return Whether the CAS operation is successful.
     * @throws IgniteInternalException with {@link Transactions#TX_STATE_STORAGE_ERR} error code in case when
     *                                 the operation has failed.
     */
    boolean compareAndSet(UUID txId, TxState txStateExpected, @NotNull TxMeta txMeta);

    /**
     * Remove the tx meta from the storage.
     *
     * @param txId Tx id.
     * @throws IgniteInternalException with {@link Transactions#TX_STATE_STORAGE_ERR} error code in case when
     *                                 the operation has failed.
     */
    void remove(UUID txId);

    /**
     * Create a cursor to scan all data in the storage.
     *
     * @return Cursor.
     */
    Cursor<IgniteBiTuple<UUID, TxMeta>> scan();

    /**
     * Removes all data from the storage and frees all resources.
     *
     * @throws IgniteInternalException with {@link Transactions#TX_STATE_STORAGE_DESTROY_ERR} error code in case when
     *                                 the operation has failed.
     */
    void destroy();

    /**
     * Create a snapshot of the storage's current state in the specified directory.
     *
     * @param snapshotPath Snapshot path.
     * @return Future which completes when the snapshot operation is complete.
     */
    CompletableFuture<Void> snapshot(Path snapshotPath);

    /**
     * Restore a storage from the snapshot.
     *
     * @param snapshotPath Snapshot path.
     */
    void restoreSnapshot(Path snapshotPath);
}
