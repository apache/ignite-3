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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.RaftGroupConfiguration;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Small abstractions for partition storages that includes only methods, mandatory for the snapshot storage.
 */
public interface PartitionAccess {
    /**
     * Returns the key that uniquely identifies the corresponding partition.
     */
    PartitionKey partitionKey();

    /**
     * Destroys and recreates the multi-versioned partition storage.
     *
     * @return Future that will complete when the partition is recreated.
     * @throws StorageException If an error has occurred during the partition destruction.
     */
    CompletableFuture<Void> reCreateMvPartitionStorage() throws StorageException;

    /**
     * Destroys and recreates the multi-versioned partition storage.
     *
     * @throws StorageException If an error has occurred during transaction state storage for the partition destruction.
     */
    void reCreateTxStatePartitionStorage() throws StorageException;

    /**
     * Creates a cursor to scan all meta of transactions.
     *
     * <p>All metas that exist at the time the method is called will be returned, in transaction ID order as an unsigned 128 bit integer.
     */
    Cursor<IgniteBiTuple<UUID, TxMeta>> getAllTxMeta();

    /**
     * Adds transaction meta.
     *
     * @param txId Transaction ID.
     * @param txMeta Transaction meta.
     */
    void addTxMeta(UUID txId, TxMeta txMeta);

    /**
     * Returns an existing row ID that is greater than or equal to the lower bound, {@code null} if not found.
     *
     * @param lowerBound Lower bound.
     * @throws StorageException If failed to read data.
     */
    @Nullable RowId closestRowId(RowId lowerBound);

    /**
     * Scans all versions of a single row.
     *
     * <p>{@link ReadResult#newestCommitTimestamp()} is NOT filled by this method for intents having preceding committed
     * versions.
     *
     * @param rowId Row id.
     * @return Cursor of results including both rows data and transaction-related context. The versions are ordered from newest to oldest.
     * @throws StorageException If failed to read data.
     */
    Cursor<ReadResult> getAllRowVersions(RowId rowId) throws StorageException;

    /**
     * Returns committed RAFT group configuration corresponding to the write command with the highest applied index, {@code null} if it was
     * never saved.
     */
    @Nullable RaftGroupConfiguration committedGroupConfiguration();

    /**
     * Creates (or replaces) an uncommitted (aka pending) version, assigned to the given transaction id. In details: - if there is no
     * uncommitted version, a new uncommitted version is added - if there is an uncommitted version belonging to the same transaction, it
     * gets replaced by the given version - if there is an uncommitted version belonging to a different transaction,
     * {@link TxIdMismatchException} is thrown
     *
     * @param rowId Row id.
     * @param row Binary row to update. Key only row means value removal.
     * @param txId Transaction id.
     * @param commitTableId Commit table id.
     * @param commitPartitionId Commit partitionId.
     * @throws TxIdMismatchException If there's another pending update associated with different transaction id.
     * @throws StorageException If failed to write data.
     */
    void addWrite(RowId rowId, BinaryRow row, UUID txId, UUID commitTableId, int commitPartitionId);

    /**
     * Creates a committed version. In details: - if there is no uncommitted version, a new committed version is added - if there is an
     * uncommitted version, this method may fail with a system exception (this method should not be called if there is already something
     * uncommitted for the given row).
     *
     * @param rowId Row id.
     * @param row Binary row to update. Key only row means value removal.
     * @param commitTimestamp Timestamp to associate with committed value.
     * @throws StorageException If failed to write data.
     */
    void addWriteCommitted(RowId rowId, BinaryRow row, HybridTimestamp commitTimestamp);

    /**
     * Updates the last applied index, term, and RAFT configuration.
     */
    void updateLastApplied(long lastAppliedIndex, long lastAppliedTerm, RaftGroupConfiguration raftGroupConfig);

    /**
     * Returns the minimum applied index of the partition storages.
     */
    long minLastAppliedIndex();

    /**
     * Returns the minimum applied term of the partition storages.
     */
    long minLastAppliedTerm();

    /**
     * Returns the maximum applied index of the partition storages.
     */
    long maxLastAppliedIndex();

    /**
     * Returns the maximum applied term of the partition storages.
     */
    long maxLastAppliedTerm();
}
