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
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.jetbrains.annotations.NotNull;

/**
 * Storage of table rows.
 */
public class InternalTableImpl implements InternalTable {
    /** Partition map. */
    private Map<Integer, RaftGroupService> partitionMap;

    /** Partitions. */
    private int partitions;

    /** Table identifier. */
    private UUID tableId;

    /**
     * @param tableId Table id.
     * @param partMap Map partition id to raft group.
     * @param partitions Partitions.
     */
    public InternalTableImpl(
        UUID tableId,
        Map<Integer, RaftGroupService> partMap,
        int partitions
    ) {
        this.tableId = tableId;
        this.partitionMap = partMap;
        this.partitions = partitions;
    }

    /**
     * Gets a table id.
     *
     * @return Table id as UUID.
     */
    @Override public @NotNull UUID tableId() {
        return tableId;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<BinaryRow> get(BinaryRow keyRow) {
        return partitionMap.get(keyRow.hash() % partitions).<SingleRowResponse>run(new GetCommand(keyRow))
            .thenApply(response -> response.getValue());
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<BinaryRow>> getAll(Collection<BinaryRow> keyRows) {
        HashMap<Integer, HashSet<BinaryRow>> setByPartition = new HashMap<>();

        for (BinaryRow keyRow : keyRows) {
            setByPartition.computeIfAbsent(keyRow.hash() % partitions, HashSet::new)
                .add(keyRow);
        }

        CompletableFuture<MultiRowsResponse>[] futures = new CompletableFuture[setByPartition.size()];

        int batchNum = 0;

        for (Map.Entry<Integer, HashSet<BinaryRow>> partToRows : setByPartition.entrySet()) {
            futures[batchNum] = partitionMap.get(partToRows.getKey()).run(new GetAllCommand(partToRows.getValue()));

            batchNum++;
        }

        CompletableFuture<Collection<BinaryRow>> future = CompletableFuture.allOf(futures)
            .thenApply(response -> Arrays.stream(futures)
                .map(CompletableFuture::join)
                .map(MultiRowsResponse::getValues)
                .flatMap(Collection::stream)
                .collect(Collectors.toList()));

        return future;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> upsert(BinaryRow row) {
        return partitionMap.get(row.hash() % partitions).run(new UpsertCommand(row));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> upsertAll(Collection<BinaryRow> rows) {
        HashMap<Integer, HashSet<BinaryRow>> setByPartition = new HashMap<>();

        for (BinaryRow keyRow : rows) {
            setByPartition.computeIfAbsent(keyRow.hash() % partitions, HashSet::new)
                .add(keyRow);
        }

        CompletableFuture<Void>[] futures = new CompletableFuture[setByPartition.size()];

        int batchNum = 0;

        for (Map.Entry<Integer, HashSet<BinaryRow>> partToRows : setByPartition.entrySet()) {
            futures[batchNum] = partitionMap.get(partToRows.getKey()).run(new UpsertAllCommand(partToRows.getValue()));

            batchNum++;
        }

        return CompletableFuture.allOf(futures);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<BinaryRow> getAndUpsert(BinaryRow row) {
        return partitionMap.get(row.hash() % partitions).<SingleRowResponse>run(new GetAndUpsertCommand(row))
            .thenApply(response -> response.getValue());
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> insert(BinaryRow row) {
        return partitionMap.get(row.hash() % partitions).run(new InsertCommand(row));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRow> rows) {
        HashMap<Integer, HashSet<BinaryRow>> setByPartition = new HashMap<>();

        for (BinaryRow keyRow : rows) {
            setByPartition.computeIfAbsent(keyRow.hash() % partitions, HashSet::new)
                .add(keyRow);
        }

        CompletableFuture<MultiRowsResponse>[] futures = new CompletableFuture[setByPartition.size()];

        int batchNum = 0;

        for (Map.Entry<Integer, HashSet<BinaryRow>> partToRows : setByPartition.entrySet()) {
            futures[batchNum] = partitionMap.get(partToRows.getKey()).run(new InsertAllCommand(partToRows.getValue()));

            batchNum++;
        }

        CompletableFuture<Collection<BinaryRow>> future = CompletableFuture.allOf(futures)
            .thenApply(response -> Arrays.stream(futures)
                .map(CompletableFuture::join)
                .map(MultiRowsResponse::getValues)
                .flatMap(Collection::stream)
                .collect(Collectors.toList()));

        return future;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replace(BinaryRow row) {
        return partitionMap.get(row.hash() % partitions).<Boolean>run(new ReplaceIfExistCommand(row));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replace(BinaryRow oldRow, BinaryRow newRow) {
        return partitionMap.get(oldRow.hash() % partitions).run(new ReplaceCommand(oldRow, newRow));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<BinaryRow> getAndReplace(BinaryRow row) {
        return partitionMap.get(row.hash() % partitions).<SingleRowResponse>run(new ReplaceIfExistCommand(row))
            .thenApply(response -> response.getValue());
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> delete(BinaryRow keyRow) {
        return partitionMap.get(keyRow.hash() % partitions).run(new DeleteCommand(keyRow));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> deleteExact(BinaryRow oldRow) {
        return partitionMap.get(oldRow.hash() % partitions).<Boolean>run(new DeleteExactCommand(oldRow));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<BinaryRow> getAndDelete(BinaryRow row) {
        return partitionMap.get(row.hash() % partitions).<SingleRowResponse>run(new GetAndDeleteCommand(row))
            .thenApply(response -> response.getValue());
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRow> rows) {
        HashMap<Integer, HashSet<BinaryRow>> setByPartition = new HashMap<>();

        for (BinaryRow keyRow : rows) {
            setByPartition.computeIfAbsent(keyRow.hash() % partitions, HashSet::new)
                .add(keyRow);
        }

        CompletableFuture<MultiRowsResponse>[] futures = new CompletableFuture[setByPartition.size()];

        int batchNum = 0;

        for (Map.Entry<Integer, HashSet<BinaryRow>> partToRows : setByPartition.entrySet()) {
            futures[batchNum] = partitionMap.get(partToRows.getKey()).run(new DeleteAllCommand(partToRows.getValue()));

            batchNum++;
        }

        CompletableFuture<Collection<BinaryRow>> future = CompletableFuture.allOf(futures)
            .thenApply(response -> Arrays.stream(futures)
                .map(CompletableFuture::join)
                .map(MultiRowsResponse::getValues)
                .flatMap(Collection::stream)
                .collect(Collectors.toList()));

        return future;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<BinaryRow>> deleteAllExact(Collection<BinaryRow> rows) {
        HashMap<Integer, HashSet<BinaryRow>> setByPartition = new HashMap<>();

        for (BinaryRow keyRow : rows) {
            setByPartition.computeIfAbsent(keyRow.hash() % partitions, HashSet::new)
                .add(keyRow);
        }

        CompletableFuture<MultiRowsResponse>[] futures = new CompletableFuture[setByPartition.size()];

        int batchNum = 0;

        for (Map.Entry<Integer, HashSet<BinaryRow>> partToRows : setByPartition.entrySet()) {
            futures[batchNum] = partitionMap.get(partToRows.getKey()).run(new DeleteExactAllCommand(partToRows.getValue()));

            batchNum++;
        }

        CompletableFuture<Collection<BinaryRow>> future = CompletableFuture.allOf(futures)
            .thenApply(response -> Arrays.stream(futures)
                .map(CompletableFuture::join)
                .map(MultiRowsResponse::getValues)
                .flatMap(Collection::stream)
                .collect(Collectors.toList()));

        return future;
    }
}
