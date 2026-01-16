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

package org.apache.ignite.internal.table;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.replicator.ReplicatorConstants.DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toZonePartitionIdMessage;
import static org.apache.ignite.internal.schema.SchemaTestUtils.specToType;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lowwatermark.TestLowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.SingleClusterNodeResolver;
import org.apache.ignite.internal.network.serialization.MessageSerializer;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.command.TimedBinaryRowMessage;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateAllCommand;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateCommand;
import org.apache.ignite.internal.partition.replicator.network.replication.BinaryRowMessage;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteMultiRowReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteSingleRowReplicaRequest;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.TestPlacementDriver;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.SchemaVersionAwareReplicaRequest;
import org.apache.ignite.internal.replicator.message.TimestampAwareReplicaResponse;
import org.apache.ignite.internal.replicator.message.ZonePartitionIdMessage;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NullBinaryRow;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.table.distributed.schema.ConstantSchemaVersions;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.table.metrics.TableMetricSource;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.impl.VolatileTxStateMetaStorage;
import org.apache.ignite.internal.tx.impl.WaitDieDeadlockPreventionPolicy;
import org.apache.ignite.internal.tx.test.TestLocalRwTxCounter;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.QualifiedNameHelper;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Enum;
import org.mockito.stubbing.Answer;

/**
 * Tests for data colocation.
 */
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
public class ItColocationTest extends BaseIgniteAbstractTest {
    /** Partitions count. */
    private static final int PARTS = 32;

    /** Keys count to check. */
    private static final int KEYS = 100;

    private static final HybridTimestampTracker observableTimestampTracker = HybridTimestampTracker.atomicTracker(null);

    private static final int ZONE_ID = 1;

    private static final int TABLE_ID = 2;

    /** Dummy internal table for tests. */
    private static InternalTable intTable;

    private static TxManager txManager;

    /** Map of the Raft commands are set by table operation. */
    private static final Int2ObjectMap<Set<Command>> CMDS_MAP = new Int2ObjectOpenHashMap<>();

    /** Message factory to create messages - RAFT commands.  */
    private static final PartitionReplicationMessagesFactory PARTITION_REPLICATION_MESSAGES_FACTORY =
            new PartitionReplicationMessagesFactory();

    /** Replica messages factory. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    @InjectConfiguration
    private static TransactionConfiguration txConfiguration;

    @InjectConfiguration
    private static SystemLocalConfiguration systemLocalConfiguration;

    @InjectConfiguration
    private static SystemDistributedConfiguration systemDistributedConfiguration;

    @InjectExecutorService
    private static ScheduledExecutorService commonExecutor;

    private SchemaDescriptor schema;

    private SchemaRegistry schemaRegistry;

    private TableViewInternal tbl;

    private TupleMarshallerImpl marshaller;

    @BeforeAll
    static void beforeAllTests() {
        InternalClusterNode clusterNode = DummyInternalTableImpl.LOCAL_NODE;

        ClusterService clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);
        when(clusterService.messagingService()).thenReturn(mock(MessagingService.class));
        when(clusterService.topologyService().localMember()).thenReturn(clusterNode);

        ReplicaService replicaService = mock(ReplicaService.class, RETURNS_DEEP_STUBS);

        RemotelyTriggeredResourceRegistry resourcesRegistry = new RemotelyTriggeredResourceRegistry();

        PlacementDriver placementDriver = new TestPlacementDriver(clusterNode);

        HybridClock clock = new HybridClockImpl();
        ClockService clockService = new TestClockService(clock);

        VolatileTxStateMetaStorage txStateVolatileStorage = VolatileTxStateMetaStorage.createStarted();

        TransactionInflights transactionInflights = new TransactionInflights(placementDriver, clockService, txStateVolatileStorage);

        txManager = new TxManagerImpl(
                txConfiguration,
                systemDistributedConfiguration,
                clusterService,
                replicaService,
                new HeapLockManager(systemLocalConfiguration, txStateVolatileStorage),
                txStateVolatileStorage,
                clockService,
                new TransactionIdGenerator(0xdeadbeef),
                placementDriver,
                () -> DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS,
                new TestLocalRwTxCounter(),
                resourcesRegistry,
                transactionInflights,
                new TestLowWatermark(),
                commonExecutor,
                new NoOpMetricManager()
        ) {
            @Override
            public CompletableFuture<Void> finish(
                    HybridTimestampTracker observableTimestampTracker,
                    ZonePartitionId commitPartition,
                    boolean commitIntent,
                    boolean timeoutExceeded,
                    boolean recovery,
                    Map<ZonePartitionId, PendingTxPartitionEnlistment> enlistedGroups,
                    UUID txId
            ) {
                return nullCompletedFuture();
            }
        };

        assertThat(txManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        Int2ObjectMap<RaftGroupService> partRafts = new Int2ObjectOpenHashMap<>();
        Map<ZonePartitionId, RaftGroupService> groupRafts = new HashMap<>();

        for (int i = 0; i < PARTS; ++i) {
            RaftGroupService r = mock(RaftGroupService.class);

            final int part = i;
            doAnswer(invocation -> {
                Command cmd = (Command) invocation.getArguments()[0];

                CMDS_MAP.merge(part, new HashSet<>(Set.of(cmd)), (newSet, set) -> {
                    set.addAll(newSet);

                    return set;
                });

                if (cmd instanceof UpdateAllCommand) {
                    return completedFuture(((UpdateAllCommand) cmd).rowsToUpdate().keySet().stream()
                            .map(uuid -> new NullBinaryRow())
                            .collect(Collectors.toList()));
                } else {
                    return trueCompletedFuture();
                }
            }).when(r).run(any());

            partRafts.put(i, r);
            groupRafts.put(new ZonePartitionId(ZONE_ID, i), r);
        }

        Answer<CompletableFuture<?>> clo = invocation -> {
            String nodeName = invocation.getArgument(0);
            InternalClusterNode node = clusterNodeByName(nodeName);
            ReplicaRequest request = invocation.getArgument(1);

            ZonePartitionIdMessage commitPartId = toZonePartitionIdMessage(REPLICA_MESSAGES_FACTORY, new ZonePartitionId(ZONE_ID, 0));

            RaftGroupService r = groupRafts.get(request.groupId().asReplicationGroupId());

            if (request instanceof ReadWriteMultiRowReplicaRequest) {
                ReadWriteMultiRowReplicaRequest multiRowReplicaRequest = (ReadWriteMultiRowReplicaRequest) request;

                Map<UUID, TimedBinaryRowMessage> rows = multiRowReplicaRequest.binaryTuples().stream()
                        .collect(
                                toMap(tupleBuffer -> TestTransactionIds.newTransactionId(),
                                        tupleBuffer -> PARTITION_REPLICATION_MESSAGES_FACTORY.timedBinaryRowMessage()
                                                .binaryRowMessage(binaryRowMessage(tupleBuffer, multiRowReplicaRequest))
                                                .build())
                        );

                return r.run(PARTITION_REPLICATION_MESSAGES_FACTORY.updateAllCommandV2()
                        .tableId(TABLE_ID)
                        .commitPartitionId(commitPartId)
                        .messageRowsToUpdate(rows)
                        .txId(UUID.randomUUID())
                        .txCoordinatorId(node.id())
                        .initiatorTime(clock.now())
                        .build());
            } else {
                assertThat(request, is(instanceOf(ReadWriteSingleRowReplicaRequest.class)));

                ReadWriteSingleRowReplicaRequest singleRowReplicaRequest = (ReadWriteSingleRowReplicaRequest) request;

                return r.run(PARTITION_REPLICATION_MESSAGES_FACTORY.updateCommandV2()
                        .tableId(TABLE_ID)
                        .commitPartitionId(commitPartId)
                        .rowUuid(UUID.randomUUID())
                        .messageRowToUpdate(PARTITION_REPLICATION_MESSAGES_FACTORY.timedBinaryRowMessage()
                                .binaryRowMessage(binaryRowMessage(singleRowReplicaRequest.binaryTuple(), singleRowReplicaRequest))
                                .build())
                        .txId(TestTransactionIds.newTransactionId())
                        .txCoordinatorId(node.id())
                        .initiatorTime(clock.now())
                        .build());
            }
        };
        when(replicaService.invoke(any(String.class), any())).thenAnswer(clo);
        when(replicaService.invokeRaw(any(String.class), any())).thenAnswer(
                invocation -> clo.answer(invocation).thenApply(res -> new TimestampAwareReplicaResponse() {
                    @Override
                    public @Nullable Object result() {
                        return res;
                    }

                    @Override
                    public @Nullable HybridTimestamp timestamp() {
                        return clock.now();
                    }

                    @Override
                    public MessageSerializer<NetworkMessage> serializer() {
                        return null;
                    }

                    @Override
                    public short messageType() {
                        return 0;
                    }

                    @Override
                    public short groupType() {
                        return 0;
                    }

                    @Override
                    public NetworkMessage clone() {
                        return null;
                    }
                }));

        intTable = new InternalTableImpl(
                QualifiedNameHelper.fromNormalized(SqlCommon.DEFAULT_SCHEMA_NAME, "TEST"),
                ZONE_ID, // zone id.
                TABLE_ID, // table id.
                PARTS, // number of partitions.
                new SingleClusterNodeResolver(clusterNode),
                txManager,
                mock(MvTableStorage.class),
                replicaService,
                clockService,
                observableTimestampTracker,
                new TestPlacementDriver(clusterNode),
                transactionInflights,
                null,
                mock(StreamerReceiverRunner.class),
                () -> 10_000L,
                () -> 10_000L,
                new TableMetricSource(QualifiedName.fromSimple("TEST"))
        );
    }

    private static InternalClusterNode clusterNodeByName(String nodeName) {
        assertThat(nodeName, is(DummyInternalTableImpl.LOCAL_NODE.name()));

        return DummyInternalTableImpl.LOCAL_NODE;
    }

    private static BinaryRowMessage binaryRowMessage(ByteBuffer tupleBuffer, SchemaVersionAwareReplicaRequest request) {
        return PARTITION_REPLICATION_MESSAGES_FACTORY.binaryRowMessage()
                .schemaVersion(request.schemaVersion())
                .binaryTuple(tupleBuffer)
                .build();
    }

    @AfterAll
    static void afterAllTests() {
        if (txManager != null) {
            assertThat(txManager.stopAsync(new ComponentContext()), willCompleteSuccessfully());
        }
    }

    @BeforeEach
    public void beforeTest() {
        CMDS_MAP.clear();
    }

    /**
     * Check colocation by two columns for all types.
     */
    @CartesianTest
    public void colocationTwoColumnsInsert(
            @Enum(names = {"NULL", "PERIOD", "DURATION", "STRUCT"}, mode = Enum.Mode.EXCLUDE) ColumnType t0,
            @Enum(names = {"NULL", "PERIOD", "DURATION", "STRUCT"}, mode = Enum.Mode.EXCLUDE) ColumnType t1
    ) {
        init(t0, t1);

        for (int i = 0; i < KEYS; ++i) {
            CMDS_MAP.clear();

            Tuple t = createTuple(i, t0, t1);

            tbl.recordView().insert(null, t);

            BinaryRowEx r = marshaller.marshal(t);

            int part = intTable.partitionId(r);

            assertThat(CollectionUtils.first(CMDS_MAP.get(part)), is(instanceOf(UpdateCommand.class)));
        }
    }

    /**
     * Check colocation by two columns for all types.
     */
    @CartesianTest
    public void colocationTwoColumnsInsertAll(
            @Enum(names = {"NULL", "PERIOD", "DURATION", "STRUCT"}, mode = Enum.Mode.EXCLUDE) ColumnType t0,
            @Enum(names = {"NULL", "PERIOD", "DURATION", "STRUCT"}, mode = Enum.Mode.EXCLUDE) ColumnType t1
    ) {
        int keysCount = t0 == ColumnType.BOOLEAN && t0 == t1 ? 2 : KEYS;

        init(t0, t1);

        tbl.recordView().insertAll(null, IntStream.range(0, keysCount).mapToObj(i -> createTuple(i, t0, t1)).collect(Collectors.toSet()));

        Int2IntMap partsMap = new Int2IntOpenHashMap();

        for (int i = 0; i < keysCount; ++i) {
            Tuple t = createTuple(i, t0, t1);

            BinaryRowEx r = marshaller.marshal(t);

            int part = intTable.partitionId(r);

            partsMap.merge(part, 1, (cnt, ignore) -> ++cnt);
        }

        assertEquals(CMDS_MAP.size(), partsMap.size());

        CMDS_MAP.forEach((p, set) -> {
            UpdateAllCommand cmd = (UpdateAllCommand) CollectionUtils.first(set);
            assertEquals(partsMap.get(p), cmd.rowsToUpdate().size(), () -> "part=" + p + ", set=" + set);

            cmd.rowsToUpdate().values().forEach(rowMessage -> {
                Row r = Row.wrapBinaryRow(schema, rowMessage.binaryRow());

                assertEquals(intTable.partitionId(r), p);
            });
        });
    }

    private void init(ColumnType t0, ColumnType t1) {
        schema = new SchemaDescriptor(1,
                List.of(
                        new Column("ID", NativeTypes.INT64, false),
                        new Column("ID0", specToType(t0), false),
                        new Column("ID1", specToType(t1), false),
                        new Column("VAL", NativeTypes.INT64, true)
                ),
                List.of("ID", "ID0", "ID1"),
                List.of("ID1", "ID0")
        );

        schemaRegistry = new DummySchemaManagerImpl(schema);

        tbl = new TableImpl(intTable, schemaRegistry, lockManager(), new ConstantSchemaVersions(1), mock(IgniteSql.class), -1);

        marshaller = new TupleMarshallerImpl(tbl::qualifiedName, schema);
    }

    private static LockManager lockManager() {
        VolatileTxStateMetaStorage txStateVolatileStorage = VolatileTxStateMetaStorage.createStarted();

        HeapLockManager lockManager = new HeapLockManager(systemLocalConfiguration, txStateVolatileStorage);
        lockManager.start(new WaitDieDeadlockPreventionPolicy());
        return lockManager;
    }

    private Tuple createTuple(int k, ColumnType t0, ColumnType t1) {
        return Tuple.create()
                .set("ID", 1L)
                .set("ID0", SqlTestUtils.generateStableValueByType(k, t0))
                .set("ID1", SqlTestUtils.generateStableValueByType(k, t1))
                .set("VAL", 0L);
    }
}
