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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.distributed.command.DeleteAllCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteExactAllCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteExactCommand;
import org.apache.ignite.internal.table.distributed.command.GetAllCommand;
import org.apache.ignite.internal.table.distributed.command.GetAndDeleteCommand;
import org.apache.ignite.internal.table.distributed.command.GetAndReplaceCommand;
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
import org.apache.ignite.internal.table.distributed.command.scan.ScanCloseCommand;
import org.apache.ignite.internal.table.distributed.command.scan.ScanInitCommand;
import org.apache.ignite.internal.table.distributed.command.scan.ScanRetrieveBatchCommand;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.IgniteUuidGenerator;
import org.apache.ignite.lang.LoggerMessageHelper;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.schema.definition.SchemaManagementMode;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

/**
 * Storage of table rows.
 */
public class InternalTableImpl implements InternalTable {
    /** Log. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(InternalTableImpl.class);

    /** IgniteUuid generator. */
    private static final IgniteUuidGenerator UUID_GENERATOR = new IgniteUuidGenerator(UUID.randomUUID(), 0);

    //TODO: IGNITE-15443 Use IntMap structure instead of HashMap.
    /** Partition map. */
    private final Map<Integer, RaftGroupService> partitionMap;

    /** Partitions. */
    private final int partitions;

    /** Table name. */
    private final String tableName;

    /** Table identifier. */
    private final IgniteUuid tableId;

    /** Resolver that resolves a network address to node id. */
    private final Function<NetworkAddress, String> netAddrResolver;

    /** Table schema mode. */
    private volatile SchemaManagementMode schemaMode;

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
        IgniteUuid tableId,
        Map<Integer, RaftGroupService> partMap,
        int partitions,
        Function<NetworkAddress, String> netAddrResolver,
        TxManager txManager
    ) {
        this.tableName = tableName;
        this.tableId = tableId;
        this.partitionMap = partMap;
        this.partitions = partitions;
        this.netAddrResolver = netAddrResolver;
        this.txManager = txManager;

        this.schemaMode = SchemaManagementMode.STRICT;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteUuid tableId() {
        return tableId;
    }

    /** {@inheritDoc} */
    @Override public String tableName() {
        return tableName;
    }

    /** {@inheritDoc} */
    @Override public SchemaManagementMode schemaMode() {
        return schemaMode;
    }

    /** {@inheritDoc} */
    @Override public void schema(SchemaManagementMode schemaMode) {
        this.schemaMode = schemaMode;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<BinaryRow> get(BinaryRow keyRow, InternalTransaction tx) {
        if (tx == null) {
            try {
                tx = txManager.tx();
            }
            catch (TransactionException e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        if (tx != null)
            return enlist(keyRow, tx).<SingleRowResponse>run(new GetCommand(keyRow, tx.timestamp())).
                thenApply(SingleRowResponse::getValue);
        else {
            InternalTransaction tx0 = txManager.begin();

            return enlist(keyRow, tx0).<SingleRowResponse>run(new GetCommand(keyRow, tx0.timestamp())).
                thenApply(SingleRowResponse::getValue).thenCompose(r -> tx0.commitAsync().thenApply(ignored -> r));
        }
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Collection<BinaryRow>> getAll(Collection<BinaryRow> keyRows, InternalTransaction tx) {
        Map<Integer, Set<BinaryRow>> keyRowsByPartition = mapRowsToPartitions(keyRows);

        CompletableFuture<MultiRowsResponse>[] futures = new CompletableFuture[keyRowsByPartition.size()];

        int batchNum = 0;

        for (Map.Entry<Integer, Set<BinaryRow>> partToRows : keyRowsByPartition.entrySet()) {
            futures[batchNum] = partitionMap.get(partToRows.getKey()).run(new GetAllCommand(partToRows.getValue()));

            batchNum++;
        }

        return collectMultiRowsResponses(futures);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> upsert(BinaryRow row, InternalTransaction tx) {
        if (tx == null) {
            try {
                tx = txManager.tx();
            }
            catch (TransactionException e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        if (tx != null)
            return enlist(row, tx).run(new UpsertCommand(row, tx.timestamp()));
        else {
            InternalTransaction tx0 = txManager.begin();

            return enlist(row, tx0).run(new UpsertCommand(row, tx0.timestamp())).thenCompose(r -> tx0.commitAsync());
        }
    }

    /**
     * @param row The row.
     * @param tx The transaction.
     * @return The corresponding raft service.
     */
    private RaftGroupService enlist(BinaryRow row, InternalTransaction tx) {
        int partId = partId(row);

        RaftGroupService svc = partitionMap.get(partId);

        // TODO asch fixme need to map to fixed topology.
        tx.enlist(svc.leader().address(), svc.groupId()); // Enlist the leaseholder.

        return svc;
    }

    /**
     * @param partId Partition id.
     * @param tx The transaction.
     * @return The corresponding raft service.
     */
    private RaftGroupService enlist(int partId, InternalTransaction tx) {
        RaftGroupService svc = partitionMap.get(partId);

        // TODO asch fixme need to map to fixed topology.
        tx.enlist(svc.leader().address(), svc.groupId()); // Enlist the leaseholder.

        return svc;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> upsertAll(Collection<BinaryRow> rows, InternalTransaction tx) {
        Map<Integer, Set<BinaryRow>> keyRowsByPartition = mapRowsToPartitions(rows);

        CompletableFuture<Void>[] futures = new CompletableFuture[keyRowsByPartition.size()];

        int batchNum = 0;

        for (Map.Entry<Integer, Set<BinaryRow>> partToRows : keyRowsByPartition.entrySet()) {
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
        if (tx == null) {
            try {
                tx = txManager.tx();
            }
            catch (TransactionException e) {
                return CompletableFuture.failedFuture(e);
            }
        }

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
        Map<Integer, Set<BinaryRow>> keyRowsByPartition = mapRowsToPartitions(rows);

        CompletableFuture<MultiRowsResponse>[] futures = new CompletableFuture[keyRowsByPartition.size()];

        int batchNum = 0;

        for (Map.Entry<Integer, Set<BinaryRow>> partToRows : keyRowsByPartition.entrySet()) {
            futures[batchNum] = partitionMap.get(partToRows.getKey()).run(new InsertAllCommand(partToRows.getValue()));

            batchNum++;
        }

        return collectMultiRowsResponses(futures);
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
        return partitionMap.get(partId(row)).<SingleRowResponse>run(new GetAndReplaceCommand(row))
            .thenApply(SingleRowResponse::getValue);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> delete(BinaryRow keyRow, InternalTransaction tx) {
        // TODO asch get rid of copy paste.
        if (tx == null) {
            try {
                tx = txManager.tx();
            }
            catch (TransactionException e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        if (tx != null) {
            return enlist(keyRow, tx).run(new DeleteCommand(keyRow, tx.timestamp()));
        }
        else {
            InternalTransaction tx0 = txManager.begin();

            return enlist(keyRow, tx0)
                .run(new DeleteCommand(keyRow, tx0.timestamp())).thenCompose(r -> tx0.commitAsync().thenApply(ignored -> (Boolean) r));
        }
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
    @Override public CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRow> rows, InternalTransaction tx) {
        Map<Integer, Set<BinaryRow>> keyRowsByPartition = mapRowsToPartitions(rows);

        CompletableFuture<MultiRowsResponse>[] futures = new CompletableFuture[keyRowsByPartition.size()];

        int batchNum = 0;

        for (Map.Entry<Integer, Set<BinaryRow>> partToRows : keyRowsByPartition.entrySet()) {
            futures[batchNum] = partitionMap.get(partToRows.getKey()).run(new DeleteAllCommand(partToRows.getValue()));

            batchNum++;
        }

        return collectMultiRowsResponses(futures);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Collection<BinaryRow>> deleteAllExact(
        Collection<BinaryRow> rows,
        InternalTransaction tx
    ) {
        Map<Integer, Set<BinaryRow>> keyRowsByPartition = mapRowsToPartitions(rows);

        CompletableFuture<MultiRowsResponse>[] futures = new CompletableFuture[keyRowsByPartition.size()];

        int batchNum = 0;

        for (Map.Entry<Integer, Set<BinaryRow>> partToRows : keyRowsByPartition.entrySet()) {
            futures[batchNum] = partitionMap.get(partToRows.getKey()).run(new DeleteExactAllCommand(partToRows.getValue()));

            batchNum++;
        }

        return collectMultiRowsResponses(futures);
    }

    /** {@inheritDoc} */
    @Override public Publisher<BinaryRow> scan(int p, InternalTransaction tx) {
        if (p < 0 || p >= partitions) {
            throw new IllegalArgumentException(
                LoggerMessageHelper.format(
                    "Invalid partition [partition={}, minValue={}, maxValue={}].",
                    p,
                    0,
                    partitions - 1
                )
            );
        }

        return new PartitionScanPublisher(partitionMap.get(p));
    }

    /**
     * Map rows to partitions.
     *
     * @param rows Rows.
     * @return Partition -%gt; rows mapping.
     */
    private Map<Integer, Set<BinaryRow>> mapRowsToPartitions(Collection<BinaryRow> rows) {
        //TODO: IGNITE-15443 Use IntMap structure instead of HashMap.
        HashMap<Integer, Set<BinaryRow>> keyRowsByPartition = new HashMap<>();

        for (BinaryRow keyRow : rows)
            keyRowsByPartition.computeIfAbsent(partId(keyRow), k -> new HashSet<>()).add(keyRow);

        return keyRowsByPartition;
    }

    /** {@inheritDoc} */
    @Override public @NotNull List<String> assignments() {
        awaitLeaderInitialization();

        return partitionMap.entrySet().stream()
            .sorted(Comparator.comparingInt(Map.Entry::getKey))
            .map(Map.Entry::getValue)
            .map(RaftGroupService::leader)
            .map(Peer::address)
            .map(netAddrResolver)
            .collect(Collectors.toList());
    }

    private void awaitLeaderInitialization() {
        List<CompletableFuture<Void>> futs = new ArrayList<>();

        for (RaftGroupService raftSvc : partitionMap.values()) {
            if (raftSvc.leader() == null)
                futs.add(raftSvc.refreshLeader());
        }

        CompletableFuture.allOf(futs.toArray(CompletableFuture[]::new)).join();
    }

    /** {@inheritDoc} */
    @TestOnly
    @Override public int partition(BinaryRow keyRow) {
        return partId(keyRow);
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

    /**
     * Collects multirow responses from multiple futures into a single collection.
     * @param futures Futures.
     * @return Row collection.
     */
    private CompletableFuture<Collection<BinaryRow>> collectMultiRowsResponses(
            CompletableFuture<MultiRowsResponse>[] futures) {
        return CompletableFuture.allOf(futures)
                .thenApply(response -> {
                    List<BinaryRow> list = new ArrayList<>(futures.length);

                    for (CompletableFuture<MultiRowsResponse> future : futures) {
                        Collection<BinaryRow> values = future.join().getValues();

                        if (values != null)
                            list.addAll(values);
                    }

                    return list;
                });
    }

    /**
     * Updates internal table raft group service for given partition.
     *
     * @param p Partition.
     * @param raftGrpSvc Raft group service.
     */
    public void updateInternalTableRaftGroupService(int p, RaftGroupService raftGrpSvc) {
        partitionMap.put(p, raftGrpSvc);
    }

    /** Partition scan publisher. */
    private class PartitionScanPublisher implements Publisher<BinaryRow> {
        /** {@link Publisher<BinaryRow>} that relatively notifies about partition rows.  */
        private final RaftGroupService raftGrpSvc;

        /** */
        private AtomicBoolean subscribed;

        /**
         * The constructor.
         *
         * @param raftGrpSvc {@link RaftGroupService} to run corresponding raft commands.
         */
        PartitionScanPublisher(RaftGroupService raftGrpSvc) {
            this.raftGrpSvc = raftGrpSvc;
            this.subscribed = new AtomicBoolean(false);
        }

        /** {@inheritDoc} */
        @Override public void subscribe(Subscriber<? super BinaryRow> subscriber) {
            if (subscriber == null)
                throw new NullPointerException("Subscriber is null");

            if (!subscribed.compareAndSet(false, true))
                subscriber.onError(new IllegalStateException("Scan publisher does not support multiple subscriptions."));

            PartitionScanSubscription subscription = new PartitionScanSubscription(subscriber);

            subscriber.onSubscribe(subscription);
        }

        /**
         * Partition Scan Subscription.
         */
        private class PartitionScanSubscription implements Subscription {
            /** */
            private final Subscriber<? super BinaryRow> subscriber;

            /** */
            private final AtomicBoolean canceled;

            /** Scan id to uniquely identify it on server side. */
            private final IgniteUuid scanId;

            /** Scan initial operation that created server cursor. */
            private final CompletableFuture<Void> scanInitOp;

            /**
             * The constructor.
             * @param subscriber The subscriber.
             */
            private PartitionScanSubscription(Subscriber<? super BinaryRow> subscriber) {
                this.subscriber = subscriber;
                this.canceled = new AtomicBoolean(false);
                this.scanId = UUID_GENERATOR.randomUuid();
                // TODO: IGNITE-15544 Close partition scans on node left.
                this.scanInitOp = raftGrpSvc.run(new ScanInitCommand("", scanId));
            }

            /** {@inheritDoc} */
            @Override public void request(long n) {
                if (n <= 0) {
                    cancel();

                    subscriber.onError(new IllegalArgumentException(LoggerMessageHelper.
                        format("Invalid requested amount of items [requested={}, minValue=1]", n))
                    );
                }

                if (canceled.get())
                    return;

                final int internalBatchSize = Integer.MAX_VALUE;

                for (int intBatchCnr = 0; intBatchCnr < (n / internalBatchSize); intBatchCnr++)
                    scanBatch(internalBatchSize);

                scanBatch((int)(n % internalBatchSize));
            }

            /** {@inheritDoc} */
            @Override public void cancel() {
                cancel(true);
            }

            /**
             * Cancels given subscription and closes cursor if necessary.
             *
             * @param closeCursor If {@code true} closes inner storage scan.
             */
            private void cancel(boolean closeCursor) {
                if (!canceled.compareAndSet(false, true))
                    return;

                if (closeCursor) {
                    scanInitOp.thenRun(() -> raftGrpSvc.run(new ScanCloseCommand(scanId))).exceptionally(closeT -> {
                        LOG.warn("Unable to close scan.", closeT);

                        return null;
                    });
                }
            }

            /**
             * Requests and processes n requested elements where n is an integer.
             *
             * @param n Requested amount of items.
             */
            private void scanBatch(int n) {
                if (canceled.get())
                    return;

                scanInitOp.thenCompose((none) -> raftGrpSvc.<MultiRowsResponse>run(new ScanRetrieveBatchCommand(n, scanId)))
                    .thenAccept(
                        res -> {
                            if (res.getValues() == null) {
                                cancel();

                                subscriber.onComplete();

                                return;
                            }
                            else
                                res.getValues().forEach(subscriber::onNext);

                            if (res.getValues().size() < n) {
                                cancel();

                                subscriber.onComplete();
                            }
                        })
                    .exceptionally(
                        t -> {
                            if (t instanceof NoSuchElementException ||
                                t instanceof CompletionException && t.getCause() instanceof NoSuchElementException)
                                return null;

                            cancel(!scanInitOp.isCompletedExceptionally());

                            subscriber.onError(t);

                            return null;
                        });
            }
        }
    }
}
