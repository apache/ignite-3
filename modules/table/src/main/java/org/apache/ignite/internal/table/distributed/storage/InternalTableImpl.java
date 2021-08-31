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

package org.apache.ignite.internal.table.distributed.storage;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.distributed.command.DeleteAllCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteExactAllCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteExactCommand;
import org.apache.ignite.internal.table.distributed.command.GetAllCommand;
import org.apache.ignite.internal.table.distributed.command.GetAndDeleteCommand;
import org.apache.ignite.internal.table.distributed.command.GetAndUpsertCommand;
import org.apache.ignite.internal.table.distributed.command.GetCommand;
import org.apache.ignite.internal.table.distributed.command.InsertAllCommand;
import org.apache.ignite.internal.table.distributed.command.InsertCommand;
import org.apache.ignite.internal.table.distributed.command.ReplaceCommand;
import org.apache.ignite.internal.table.distributed.command.ReplaceIfExistCommand;
import org.apache.ignite.internal.table.distributed.command.UpsertAllCommand;
import org.apache.ignite.internal.table.distributed.command.UpsertCommand;
import org.apache.ignite.internal.table.distributed.command.response.MultiRowsResponse;
import org.apache.ignite.internal.table.distributed.command.response.SingleRowResponse;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.schema.SchemaMode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

/**
 * Storage of table rows.
 */
public class InternalTableImpl implements InternalTable {
    /** Partition map. */
    private Map<Integer, RaftGroupService> partitionMap;

    /** Partitions. */
    private int partitions;

    /** Table name. */
    private String tableName;

    /** Table identifier. */
    private UUID tableId;

    /** Table schema mode. */
    private volatile SchemaMode schemaMode;

    /** TX manager. */
    private final TxManager txManager;

    /**
     * @param tableName Table name.
     * @param tableId Table id.
     * @param partMap Map partition id to raft group.
     * @param partitions Partitions.
     * @param txManager Transaction manager.
     */
    public InternalTableImpl(
        String tableName,
        UUID tableId,
        Map<Integer, RaftGroupService> partMap,
        int partitions,
        TxManager txManager) {
        this.tableName = tableName;
        this.tableId = tableId;
        this.partitionMap = partMap;
        this.partitions = partitions;
        this.txManager = txManager;

        this.schemaMode = SchemaMode.STRICT_SCHEMA;
    }

    /** {@inheritDoc} */
    @Override public @NotNull UUID tableId() {
        return tableId;
    }

    /** {@inheritDoc} */
    @Override public String tableName() {
        return tableName;
    }

    /** {@inheritDoc} */
    @Override public SchemaMode schemaMode() {
        return schemaMode;
    }

    /** {@inheritDoc} */
    @Override public void schema(SchemaMode schemaMode) {
        this.schemaMode = schemaMode;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<BinaryRow> get(BinaryRow keyRow, InternalTransaction tx) {
        return enlist(keyRow, tx).<SingleRowResponse>run(new GetCommand(keyRow, tx.timestamp()))
            .thenApply(SingleRowResponse::getValue);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Collection<BinaryRow>> getAll(Collection<BinaryRow> keyRows,
        InternalTransaction tx) {
        HashMap<Integer, HashSet<BinaryRow>> keyRowsByPartition = new HashMap<>();

        for (BinaryRow keyRow : keyRows)
            keyRowsByPartition.computeIfAbsent(partId(keyRow), HashSet::new).add(keyRow);

        CompletableFuture<MultiRowsResponse>[] futures = new CompletableFuture[keyRowsByPartition.size()];

        int batchNum = 0;

        for (Map.Entry<Integer, HashSet<BinaryRow>> partToRows : keyRowsByPartition.entrySet()) {
            futures[batchNum] = partitionMap.get(partToRows.getKey()).run(new GetAllCommand(partToRows.getValue()));

            batchNum++;
        }

        return CompletableFuture.allOf(futures)
            .thenApply(response -> Arrays.stream(futures)
                .map(CompletableFuture::join)
                .map(MultiRowsResponse::getValues)
                .flatMap(Collection::stream)
                .collect(Collectors.toList()));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> upsert(BinaryRow row, InternalTransaction tx) {
        if (tx != null)
            return enlist(row, tx).run(new UpsertCommand(row, tx.timestamp()));
        else {
            InternalTransaction tx0 = txManager.begin();

            return enlist(row, tx0).run(new UpsertCommand(row, tx0.timestamp())).thenCompose(r -> tx0.commitAsync());
        }
    }

    private RaftGroupService enlist(BinaryRow row, InternalTransaction tx) {
        RaftGroupService svc = partitionMap.get(partId(row));

        // TODO asch fixme need to map to fixed topology.
        tx.enlist(svc.leader().address()); // Enlist the leaseholder.

        return svc;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> upsertAll(Collection<BinaryRow> rows, InternalTransaction tx) {
        HashMap<Integer, HashSet<BinaryRow>> keyRowsByPartition = new HashMap<>();

        for (BinaryRow keyRow : rows) {
            keyRowsByPartition.computeIfAbsent(partId(keyRow), HashSet::new)
                .add(keyRow);
        }

        CompletableFuture<Void>[] futures = new CompletableFuture[keyRowsByPartition.size()];

        int batchNum = 0;

        for (Map.Entry<Integer, HashSet<BinaryRow>> partToRows : keyRowsByPartition.entrySet()) {
            futures[batchNum] = partitionMap.get(partToRows.getKey()).run(new UpsertAllCommand(partToRows.getValue()));

            batchNum++;
        }

        return CompletableFuture.allOf(futures);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<BinaryRow> getAndUpsert(BinaryRow row, InternalTransaction tx) {
        return partitionMap.get(partId(row)).<SingleRowResponse>run(new GetAndUpsertCommand(row))
            .thenApply(SingleRowResponse::getValue);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> insert(BinaryRow row, InternalTransaction tx) {
        if (tx != null) {
            return enlist(row, tx).run(new InsertCommand(row, tx.timestamp()));
        }
        else {
            InternalTransaction tx0 = txManager.begin();

            return enlist(row, tx0)
                .run(new InsertCommand(row, tx0.timestamp())).thenCompose(r -> tx0.commitAsync().thenApply(ignored -> (Boolean) r));
        }
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRow> rows, InternalTransaction tx) {
        HashMap<Integer, HashSet<BinaryRow>> keyRowsByPartition = new HashMap<>();

        for (BinaryRow keyRow : rows) {
            keyRowsByPartition.computeIfAbsent(partId(keyRow), HashSet::new)
                .add(keyRow);
        }

        CompletableFuture<MultiRowsResponse>[] futures = new CompletableFuture[keyRowsByPartition.size()];

        int batchNum = 0;

        for (Map.Entry<Integer, HashSet<BinaryRow>> partToRows : keyRowsByPartition.entrySet()) {
            futures[batchNum] = partitionMap.get(partToRows.getKey()).run(new InsertAllCommand(partToRows.getValue()));

            batchNum++;
        }

        return CompletableFuture.allOf(futures)
            .thenApply(response -> Arrays.stream(futures)
                .map(CompletableFuture::join)
                .map(MultiRowsResponse::getValues)
                .flatMap(Collection::stream)
                .collect(Collectors.toList()));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> replace(BinaryRow row, InternalTransaction tx) {
        return partitionMap.get(partId(row)).<Boolean>run(new ReplaceIfExistCommand(row));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> replace(BinaryRow oldRow, BinaryRow newRow,
        InternalTransaction tx) {
        return partitionMap.get(partId(oldRow)).run(new ReplaceCommand(oldRow, newRow));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<BinaryRow> getAndReplace(BinaryRow row, InternalTransaction tx) {
        return partitionMap.get(partId(row)).<SingleRowResponse>run(new ReplaceIfExistCommand(row))
            .thenApply(SingleRowResponse::getValue);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> delete(BinaryRow keyRow, InternalTransaction tx) {
        return partitionMap.get(partId(keyRow)).run(new DeleteCommand(keyRow));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> deleteExact(BinaryRow oldRow, InternalTransaction tx) {
        return partitionMap.get(partId(oldRow)).<Boolean>run(new DeleteExactCommand(oldRow));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<BinaryRow> getAndDelete(BinaryRow row, InternalTransaction tx) {
        return partitionMap.get(partId(row)).<SingleRowResponse>run(new GetAndDeleteCommand(row))
            .thenApply(SingleRowResponse::getValue);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRow> rows,
        InternalTransaction tx) {
        HashMap<Integer, HashSet<BinaryRow>> keyRowsByPartition = new HashMap<>();

        for (BinaryRow keyRow : rows) {
            keyRowsByPartition.computeIfAbsent(partId(keyRow), HashSet::new)
                .add(keyRow);
        }

        CompletableFuture<MultiRowsResponse>[] futures = new CompletableFuture[keyRowsByPartition.size()];

        int batchNum = 0;

        for (Map.Entry<Integer, HashSet<BinaryRow>> partToRows : keyRowsByPartition.entrySet()) {
            futures[batchNum] = partitionMap.get(partToRows.getKey()).run(new DeleteAllCommand(partToRows.getValue()));

            batchNum++;
        }

        return CompletableFuture.allOf(futures)
            .thenApply(response -> Arrays.stream(futures)
                .map(CompletableFuture::join)
                .map(MultiRowsResponse::getValues)
                .flatMap(Collection::stream)
                .collect(Collectors.toList()));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Collection<BinaryRow>> deleteAllExact(Collection<BinaryRow> rows,
        InternalTransaction tx) {
        HashMap<Integer, HashSet<BinaryRow>> keyRowsByPartition = new HashMap<>();

        for (BinaryRow keyRow : rows) {
            keyRowsByPartition.computeIfAbsent(partId(keyRow), HashSet::new)
                .add(keyRow);
        }

        CompletableFuture<MultiRowsResponse>[] futures = new CompletableFuture[keyRowsByPartition.size()];

        int batchNum = 0;

        for (Map.Entry<Integer, HashSet<BinaryRow>> partToRows : keyRowsByPartition.entrySet()) {
            futures[batchNum] = partitionMap.get(partToRows.getKey()).run(new DeleteExactAllCommand(partToRows.getValue()));

            batchNum++;
        }

        return CompletableFuture.allOf(futures)
            .thenApply(response -> Arrays.stream(futures)
                .map(CompletableFuture::join)
                .map(MultiRowsResponse::getValues)
                .flatMap(Collection::stream)
                .collect(Collectors.toList()));
    }

    /**
     * Get partition id by key row.
     *
     * @param row Key row.
     * @return partition id.
     */
    private int partId(BinaryRow row) {
        int partId = row.hash() % partitions;

        return (partId < 0) ? -partId : partId;
    }

    /**
     * @return Transaction manager.
     */
    @TestOnly
    public TxManager transactionManager() {
        return txManager;
    }
}
