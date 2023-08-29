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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import javax.naming.OperationNotSupportedException;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.internal.TestHybridClock;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.table.distributed.HashIndexLocker;
import org.apache.ignite.internal.table.distributed.IndexLocker;
import org.apache.ignite.internal.table.distributed.LowWatermark;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.TableIndexStoragesSupplier;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.table.distributed.gc.GcUpdateHandler;
import org.apache.ignite.internal.table.distributed.index.IndexBuilder;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener;
import org.apache.ignite.internal.table.distributed.replicator.PlacementDriver;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateTableStorage;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeImpl;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * Dummy table storage implementation.
 */
public class DummyInternalTableImpl extends InternalTableImpl {
    public static final IgniteLogger LOG = Loggers.forClass(DummyInternalTableImpl.class);

    public static final NetworkAddress ADDR = new NetworkAddress("127.0.0.1", 2004);

    public static final HybridClock CLOCK = new TestHybridClock(new LongSupplier() {
        @Override
        public long getAsLong() {
            return 0;
        }
    });

    private static final int PART_ID = 0;

    private static final SchemaDescriptor SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("key", NativeTypes.INT64, false)},
            new Column[]{new Column("value", NativeTypes.INT64, false)}
    );

    private static final ReplicationGroupId crossTableGroupId = new TablePartitionId(333, 0);

    private PartitionListener partitionListener;

    private ReplicaListener replicaListener;

    private final ReplicationGroupId groupId;

    /** The thread updates safe time on the dummy replica. */
    private final PendingComparableValuesTracker<HybridTimestamp, Void> safeTime;

    private static final AtomicInteger nextTableId = new AtomicInteger(10_001);

    /**
     * Creates a new local table.
     *
     * @param replicaSvc Replica service.
     */
    public DummyInternalTableImpl(ReplicaService replicaSvc) {
        this(replicaSvc, SCHEMA);
    }

    /**
     * Creates a new local table.
     *
     * @param replicaSvc Replica service.
     * @param schema Schema.
     */
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
                nextTableId.getAndIncrement(),
                Int2ObjectMaps.singleton(PART_ID, mock(RaftGroupService.class)),
                1,
                name -> mockClusterNode(),
                txManager == null
                        ? new TxManagerImpl(replicaSvc, new HeapLockManager(), CLOCK, new TransactionIdGenerator(0xdeadbeef), () -> "local")
                        : txManager,
                mock(MvTableStorage.class),
                new TestTxStateTableStorage(),
                replicaSvc,
                CLOCK
        );
        RaftGroupService svc = raftGroupServiceByPartitionId.get(0);

        groupId = crossTableUsage ? new TablePartitionId(tableId(), PART_ID) : crossTableGroupId;

        lenient().doReturn(groupId).when(svc).groupId();
        Peer leaderPeer = new Peer(UUID.randomUUID().toString());
        lenient().doReturn(leaderPeer).when(svc).leader();
        lenient().doReturn(completedFuture(new LeaderWithTerm(leaderPeer, 1L))).when(svc).refreshAndGetLeaderWithTerm();

        if (!crossTableUsage) {
            // Delegate replica requests directly to replica listener.
            lenient()
                    .doAnswer(invocationOnMock -> {
                        ClusterNode node = invocationOnMock.getArgument(0);

                        return replicaListener.invoke(invocationOnMock.getArgument(1), node.id());
                    })
                    .when(replicaSvc).invoke(any(ClusterNode.class), any());
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

        int tableId = tableId();
        int indexId = 1;

        ColumnsExtractor row2Tuple = BinaryRowConverter.keyExtractor(schema);

        Lazy<TableSchemaAwareIndexStorage> pkStorage = new Lazy<>(() -> new TableSchemaAwareIndexStorage(
                indexId,
                new TestHashIndexStorage(PART_ID, null),
                row2Tuple
        ));

        IndexLocker pkLocker = new HashIndexLocker(indexId, true, this.txManager.lockManager(), row2Tuple);

        safeTime = mock(PendingComparableValuesTracker.class);
        PartitionDataStorage partitionDataStorage = new TestPartitionDataStorage(mvPartStorage);
        TableIndexStoragesSupplier indexes = createTableIndexStoragesSupplier(Map.of(pkStorage.get().id(), pkStorage.get()));

        GcConfiguration gcConfig = mock(GcConfiguration.class);
        ConfigurationValue<Integer> gcBatchSizeValue = mock(ConfigurationValue.class);
        lenient().when(gcBatchSizeValue.value()).thenReturn(5);
        lenient().when(gcConfig.onUpdateBatchSize()).thenReturn(gcBatchSizeValue);

        IndexUpdateHandler indexUpdateHandler = new IndexUpdateHandler(indexes);

        StorageUpdateHandler storageUpdateHandler = new StorageUpdateHandler(
                PART_ID,
                partitionDataStorage,
                gcConfig,
                mock(LowWatermark.class),
                indexUpdateHandler,
                new GcUpdateHandler(partitionDataStorage, safeTime, indexUpdateHandler)
        );

        DummySchemaManagerImpl schemaManager = new DummySchemaManagerImpl(schema);

        replicaListener = new PartitionReplicaListener(
                mvPartStorage,
                raftGroupServiceByPartitionId.get(PART_ID),
                this.txManager,
                this.txManager.lockManager(),
                Runnable::run,
                PART_ID,
                tableId,
                () -> Map.of(pkLocker.id(), pkLocker),
                pkStorage,
                Map::of,
                CLOCK,
                safeTime,
                txStateStorage().getOrCreateTxStateStorage(PART_ID),
                placementDriver,
                storageUpdateHandler,
                new DummySchemas(schemaManager),
                mockClusterNode(),
                mock(MvTableStorage.class),
                mock(IndexBuilder.class),
                mock(SchemaSyncService.class, invocation -> completedFuture(null)),
                mock(CatalogService.class),
                mock(TablesConfiguration.class)
        );

        lenient().when(safeTime.waitFor(any())).thenReturn(completedFuture(null));
        lenient().when(safeTime.current()).thenReturn(new HybridTimestamp(1, 0));

        partitionListener = new PartitionListener(
                this.txManager,
                new TestPartitionDataStorage(mvPartStorage),
                storageUpdateHandler,
                txStateStorage().getOrCreateTxStateStorage(PART_ID),
                safeTime,
                new PendingComparableValuesTracker<>(0L)
        );
    }

    private static ClusterNode mockClusterNode() {
        return new ClusterNodeImpl("id", "node", new NetworkAddress("127.0.0.1", 20000));
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

    /**
     * Gets the transaction manager that is bound to the table.
     *
     * @return Transaction manager.
     */
    public TxManager txManager() {
        return txManager;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> get(BinaryRowEx keyRow, InternalTransaction tx) {
        return super.get(keyRow, tx);
    }

    /** {@inheritDoc} */
    @Override
    public List<String> assignments() {
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
        return completedFuture(mockClusterNode());
    }

    /**
     * Returns dummy table index storages supplier.
     *
     * @param indexes Index storage by ID.
     */
    public static TableIndexStoragesSupplier createTableIndexStoragesSupplier(Map<Integer, TableSchemaAwareIndexStorage> indexes) {
        return new TableIndexStoragesSupplier() {
            @Override
            public Map<Integer, TableSchemaAwareIndexStorage> get() {
                return indexes;
            }

            @Override
            public void addIndexToWaitIfAbsent(int indexId) {
            }
        };
    }
}
