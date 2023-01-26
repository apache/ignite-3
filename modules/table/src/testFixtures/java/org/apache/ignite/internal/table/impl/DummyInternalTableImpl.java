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

package org.apache.ignite.internal.table.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.naming.OperationNotSupportedException;
import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.schema.BinaryConverter;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.TableRowConverter;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.table.distributed.HashIndexLocker;
import org.apache.ignite.internal.table.distributed.IndexLocker;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener;
import org.apache.ignite.internal.table.distributed.replicator.PlacementDriver;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateTableStorage;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Dummy table storage implementation.
 */
public class DummyInternalTableImpl extends InternalTableImpl {
    public static final NetworkAddress ADDR = new NetworkAddress("127.0.0.1", 2004);

    private static final int PART_ID = 0;

    private static final SchemaDescriptor SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("key", NativeTypes.INT64, false)},
            new Column[]{new Column("value", NativeTypes.INT64, false)}
    );

    private static final ReplicationGroupId crossTableGroupId = new TablePartitionId(UUID.randomUUID(), 0);

    private PartitionListener partitionListener;

    private ReplicaListener replicaListener;

    private ReplicationGroupId groupId;

    /**
     * Creates a new local table.
     *
     * @param replicaSvc Replica service.
     */
    public DummyInternalTableImpl(ReplicaService replicaSvc) {
        this(replicaSvc, SCHEMA);
    }

    public DummyInternalTableImpl(ReplicaService replicaSvc, SchemaDescriptor schema) {
        this(replicaSvc, new TestMvPartitionStorage(0), schema);
    }

    /**
     * Creates a new local table.
     *
     * @param replicaSvc Replica service.
     * @param txManager Transaction manager.
     * @param crossTableUsage If this dummy table is going to be used in cross-table tests, it won't mock the calls of ReplicaService
     *                        by itself.
     * @param placementDriver Placement driver.
     * @param schema Schema descriptor.
     */
    public DummyInternalTableImpl(
            ReplicaService replicaSvc,
            TxManager txManager,
            boolean crossTableUsage,
            PlacementDriver placementDriver,
            SchemaDescriptor schema
    ) {
        this(replicaSvc, new TestMvPartitionStorage(0), txManager, crossTableUsage, placementDriver, schema);
    }

    /**
     * Creates a new local table.
     *
     * @param replicaSvc Replica service.
     * @param mvPartStorage Multi version partition storage.
     * @param schema Schema descriptor.
     */
    public DummyInternalTableImpl(ReplicaService replicaSvc, MvPartitionStorage mvPartStorage, SchemaDescriptor schema) {
        this(replicaSvc, mvPartStorage, null, false, null, schema);
    }

    /**
     * Creates a new local table.
     *
     * @param replicaSvc Replica service.
     * @param mvPartStorage Multi version partition storage.
     * @param txManager Transaction manager, if {@code null}, then default one will be created.
     * @param crossTableUsage If this dummy table is going to be used in cross-table tests, it won't mock the calls of ReplicaService
     *                        by itself.
     * @param placementDriver Placement driver.
     * @param schema Schema descriptor.
     */
    public DummyInternalTableImpl(
            ReplicaService replicaSvc,
            MvPartitionStorage mvPartStorage,
            @Nullable TxManager txManager,
            boolean crossTableUsage,
            PlacementDriver placementDriver,
            SchemaDescriptor schema
    ) {
        super(
                "test",
                UUID.randomUUID(),
                1,
                Int2ObjectMaps.singleton(PART_ID, mock(RaftGroupService.class)),
                1,
                name -> mock(ClusterNode.class),
                txManager == null ? new TxManagerImpl(replicaSvc, new HeapLockManager(), new HybridClockImpl()) : txManager,
                mock(MvTableStorage.class),
                new TestTxStateTableStorage(),
                replicaSvc,
                new HybridClockImpl()
        );
        RaftGroupService svc = partitionMap.get(0);

        groupId = crossTableUsage ? new TablePartitionId(tableId(), PART_ID) : crossTableGroupId;

        lenient().doReturn(groupId).when(svc).groupId();
        Peer leaderPeer = new Peer(UUID.randomUUID().toString());
        lenient().doReturn(leaderPeer).when(svc).leader();
        lenient().doReturn(CompletableFuture.completedFuture(new LeaderWithTerm(leaderPeer, 1L))).when(svc).refreshAndGetLeaderWithTerm();

        if (!crossTableUsage) {
            // Delegate replica requests directly to replica listener.
            lenient().doAnswer(
                    invocationOnMock -> {
                        CompletableFuture<Object> invoke = replicaListener.invoke(invocationOnMock.getArgument(1));
                        return invoke;
                    }
            ).when(replicaSvc).invoke(any(), any());
        }

        AtomicLong raftIndex = new AtomicLong();

        // Delegate directly to listener.
        lenient().doAnswer(
                invocationClose -> {
                    Command cmd = invocationClose.getArgument(0);

                    long commandIndex = raftIndex.incrementAndGet();

                    CompletableFuture<Serializable> res = new CompletableFuture<>();

                    // All read commands are handled directly throw partition replica listener.
                    CommandClosure<WriteCommand> clo = new CommandClosure<>() {
                        /** {@inheritDoc} */
                        @Override
                        public long index() {
                            return commandIndex;
                        }

                        /** {@inheritDoc} */
                        @Override
                        public WriteCommand command() {
                            return (WriteCommand) cmd;
                        }

                        /** {@inheritDoc} */
                        @Override
                        public void result(@Nullable Serializable r) {
                            if (r instanceof Throwable) {
                                res.completeExceptionally((Throwable) r);
                            } else {
                                res.complete(r);
                            }
                        }
                    };

                    try {
                        partitionListener.onWrite(List.of(clo).iterator());
                    } catch (Throwable e) {
                        res.completeExceptionally(new TransactionException(e));
                    }

                    return res;
                }
        ).when(svc).run(any());

        UUID tableId = tableId();
        UUID indexId = UUID.randomUUID();

        BinaryTupleSchema tupleSchema = BinaryTupleSchema.createRowSchema(schema);
        BinaryTupleSchema indexSchema = BinaryTupleSchema.createKeySchema(schema);

        BinaryConverter binaryRowConverter = BinaryConverter.forKey(schema);
        Function<BinaryRow, BinaryTuple> binaryRow2Tuple = binaryRowConverter::toTuple;

        var rowConverter = new TableRowConverter(tupleSchema, indexSchema);

        Lazy<TableSchemaAwareIndexStorage> pkStorage = new Lazy<>(() -> new TableSchemaAwareIndexStorage(
                indexId,
                new TestHashIndexStorage(null),
                binaryRow2Tuple,
                rowConverter::toTuple
        ));

        IndexLocker pkLocker = new HashIndexLocker(indexId, true, this.txManager.lockManager(), binaryRow2Tuple);

        HybridClock clock = new HybridClockImpl();
        PendingComparableValuesTracker<HybridTimestamp> safeTime = new PendingComparableValuesTracker<>(clock.now());
        PartitionDataStorage partitionDataStorage = new TestPartitionDataStorage(mvPartStorage);
        Supplier<Map<UUID, TableSchemaAwareIndexStorage>> indexes = () -> Map.of(pkStorage.get().id(), pkStorage.get());
        StorageUpdateHandler storageUpdateHandler = new StorageUpdateHandler(PART_ID, partitionDataStorage, indexes);

        DummySchemaManagerImpl schemaManager = new DummySchemaManagerImpl(schema);

        replicaListener = new PartitionReplicaListener(
                mvPartStorage,
                partitionMap.get(PART_ID),
                this.txManager,
                this.txManager.lockManager(),
                Runnable::run,
                PART_ID,
                tableId,
                () -> Map.of(pkLocker.id(), pkLocker),
                pkStorage,
                () -> Map.of(),
                clock,
                safeTime,
                txStateStorage().getOrCreateTxStateStorage(PART_ID),
                placementDriver,
                storageUpdateHandler,
                peer -> true,
                CompletableFuture.completedFuture(schemaManager)
        );

        partitionListener = new PartitionListener(
                new TestPartitionDataStorage(mvPartStorage),
                storageUpdateHandler,
                txStateStorage().getOrCreateTxStateStorage(PART_ID),
                safeTime
        );
    }

    /**
     * Replica listener.
     *
     * @return Replica listener.
     */
    public ReplicaListener getReplicaListener() {
        return replicaListener;
    }

    /**
     * Group id of single partition of this table.
     *
     * @return Group id.
     */
    public ReplicationGroupId groupId() {
        return groupId;
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> get(BinaryRowEx keyRow, InternalTransaction tx) {
        return super.get(keyRow, tx);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull List<String> assignments() {
        throw new IgniteInternalException(new OperationNotSupportedException());
    }

    /** {@inheritDoc} */
    @Override
    public int partition(BinaryRowEx keyRow) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<ClusterNode> evaluateReadOnlyRecipientNode(int partId) {
        return CompletableFuture.completedFuture(mock(ClusterNode.class));
    }
}
