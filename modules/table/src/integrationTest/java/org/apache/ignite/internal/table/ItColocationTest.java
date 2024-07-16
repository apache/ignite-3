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
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toTablePartitionIdMessage;
import static org.apache.ignite.internal.schema.SchemaTestUtils.specToType;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lowwatermark.TestLowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.SingleClusterNodeResolver;
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
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.SchemaVersionAwareReplicaRequest;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NullBinaryRow;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.table.distributed.schema.ConstantSchemaVersions;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.distributed.storage.TableRaftServiceImpl;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateTableStorage;
import org.apache.ignite.internal.tx.test.TestLocalRwTxCounter;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for data colocation.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItColocationTest extends BaseIgniteAbstractTest {
    /** Partitions count. */
    private static final int PARTS = 32;

    /** Keys count to check. */
    private static final int KEYS = 100;

    private static final HybridTimestampTracker observableTimestampTracker = new HybridTimestampTracker();

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

    private SchemaDescriptor schema;

    private SchemaRegistry schemaRegistry;

    private TableViewInternal tbl;

    private TupleMarshallerImpl marshaller;

    @BeforeAll
    static void beforeAllTests() {
        ClusterNode clusterNode = DummyInternalTableImpl.LOCAL_NODE;

        ClusterService clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);
        when(clusterService.messagingService()).thenReturn(mock(MessagingService.class));
        when(clusterService.topologyService().localMember()).thenReturn(clusterNode);

        ReplicaService replicaService = mock(ReplicaService.class, RETURNS_DEEP_STUBS);

        RemotelyTriggeredResourceRegistry resourcesRegistry = new RemotelyTriggeredResourceRegistry();

        PlacementDriver placementDriver = new TestPlacementDriver(clusterNode);

        HybridClock clock = new HybridClockImpl();
        ClockService clockService = new TestClockService(clock);

        TransactionInflights transactionInflights = new TransactionInflights(placementDriver, clockService);

        txManager = new TxManagerImpl(
                txConfiguration,
                clusterService,
                replicaService,
                new HeapLockManager(),
                clockService,
                new TransactionIdGenerator(0xdeadbeef),
                placementDriver,
                () -> DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS,
                new TestLocalRwTxCounter(),
                resourcesRegistry,
                transactionInflights,
                new TestLowWatermark()
        ) {
            @Override
            public CompletableFuture<Void> finish(
                    HybridTimestampTracker observableTimestampTracker,
                    TablePartitionId commitPartition,
                    boolean commitIntent,
                    Map<TablePartitionId, IgniteBiTuple<ClusterNode, Long>> enlistedGroups,
                    UUID txId
            ) {
                return nullCompletedFuture();
            }
        };

        assertThat(txManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        Int2ObjectMap<RaftGroupService> partRafts = new Int2ObjectOpenHashMap<>();
        Map<ReplicationGroupId, RaftGroupService> groupRafts = new HashMap<>();

        int tblId = 1;

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
            groupRafts.put(new TablePartitionId(tblId, i), r);
        }

        when(replicaService.invoke(any(ClusterNode.class), any())).thenAnswer(invocation -> {
            ClusterNode node = invocation.getArgument(0);
            ReplicaRequest request = invocation.getArgument(1);
            var commitPartId = new TablePartitionId(2, 0);

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

                return r.run(PARTITION_REPLICATION_MESSAGES_FACTORY.updateAllCommand()
                        .tablePartitionId(toTablePartitionIdMessage(REPLICA_MESSAGES_FACTORY, commitPartId))
                        .messageRowsToUpdate(rows)
                        .txId(UUID.randomUUID())
                        .txCoordinatorId(node.id())
                        .build());
            } else {
                assertThat(request, is(instanceOf(ReadWriteSingleRowReplicaRequest.class)));

                ReadWriteSingleRowReplicaRequest singleRowReplicaRequest = (ReadWriteSingleRowReplicaRequest) request;

                return r.run(PARTITION_REPLICATION_MESSAGES_FACTORY.updateCommand()
                        .tablePartitionId(toTablePartitionIdMessage(REPLICA_MESSAGES_FACTORY, commitPartId))
                        .rowUuid(UUID.randomUUID())
                        .messageRowToUpdate(PARTITION_REPLICATION_MESSAGES_FACTORY.timedBinaryRowMessage()
                                .binaryRowMessage(binaryRowMessage(singleRowReplicaRequest.binaryTuple(), singleRowReplicaRequest))
                                .build())
                        .txId(TestTransactionIds.newTransactionId())
                        .txCoordinatorId(node.id())
                        .build());
            }
        });

        intTable = new InternalTableImpl(
                "PUBLIC.TEST",
                tblId,
                PARTS,
                new SingleClusterNodeResolver(clusterNode),
                txManager,
                mock(MvTableStorage.class),
                new TestTxStateTableStorage(),
                replicaService,
                new HybridClockImpl(),
                observableTimestampTracker,
                new TestPlacementDriver(clusterNode),
                new TableRaftServiceImpl("PUBLIC.TEST", PARTS, partRafts, new SingleClusterNodeResolver(clusterNode)),
                transactionInflights,
                3_000,
                0,
                null,
                null
        );
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

    private static Object generateValueByType(int i, NativeTypeSpec type) {
        switch (type) {
            case BOOLEAN:
                return i % 2 == 0;
            case INT8:
                return (byte) i;
            case INT16:
                return (short) i;
            case INT32:
                return i;
            case INT64:
                return (long) i;
            case FLOAT:
                return (float) i + ((float) i / 1000);
            case DOUBLE:
                return (double) i + ((double) i / 1000);
            case DECIMAL:
                return BigDecimal.valueOf((double) i + ((double) i / 1000));
            case UUID:
                return new UUID(i, i);
            case STRING:
                return "str_" + i;
            case BYTES:
                return new byte[]{(byte) i, (byte) (i + 1), (byte) (i + 2)};
            case BITMASK:
                return BitSet.valueOf(new byte[]{(byte) i, (byte) (i + 1)});
            case NUMBER:
                return BigInteger.valueOf(i);
            case DATE:
                return LocalDate.of(2022, 01, 01).plusDays(i);
            case TIME:
                return LocalTime.of(0, 00, 00).plusSeconds(i);
            case DATETIME:
                return LocalDateTime.of(
                        (LocalDate) generateValueByType(i, NativeTypeSpec.DATE),
                        (LocalTime) generateValueByType(i, NativeTypeSpec.TIME)
                );
            case TIMESTAMP:
                return ((LocalDateTime) generateValueByType(i, NativeTypeSpec.DATETIME))
                        .atZone(ZoneId.systemDefault())
                        .toInstant();
            default:
                throw new IllegalStateException("Unexpected type: " + type);
        }
    }

    private static Stream<Arguments> twoColumnsParameters() {
        List<Arguments> args = new ArrayList<>();

        for (NativeTypeSpec t0 : NativeTypeSpec.values()) {
            for (NativeTypeSpec t1 : NativeTypeSpec.values()) {
                args.add(Arguments.of(t0, t1));
            }
        }

        return args.stream();
    }

    /**
     * Check colocation by two columns for all types.
     */
    @ParameterizedTest(name = "types=" + ARGUMENTS_PLACEHOLDER)
    @MethodSource("twoColumnsParameters")
    public void colocationTwoColumnsInsert(NativeTypeSpec t0, NativeTypeSpec t1) {
        init(t0, t1);

        for (int i = 0; i < KEYS; ++i) {
            CMDS_MAP.clear();

            Tuple t = createTuple(i, t0, t1);

            tbl.recordView().insert(null, t);

            BinaryRowEx r = marshaller.marshal(t);

            int part = intTable.partition(r);

            assertThat(CollectionUtils.first(CMDS_MAP.get(part)), is(instanceOf(UpdateCommand.class)));
        }
    }

    /**
     * Check colocation by two columns for all types.
     */
    @ParameterizedTest(name = "types=" + ARGUMENTS_PLACEHOLDER)
    @MethodSource("twoColumnsParameters")
    public void colocationTwoColumnsInsertAll(NativeTypeSpec t0, NativeTypeSpec t1) {
        int keysCount = t0 == NativeTypeSpec.BOOLEAN && t0 == t1 ? 2 : KEYS;

        init(t0, t1);

        tbl.recordView().insertAll(null, IntStream.range(0, keysCount).mapToObj(i -> createTuple(i, t0, t1)).collect(Collectors.toSet()));

        Int2IntMap partsMap = new Int2IntOpenHashMap();

        for (int i = 0; i < keysCount; ++i) {
            Tuple t = createTuple(i, t0, t1);

            BinaryRowEx r = marshaller.marshal(t);

            int part = intTable.partition(r);

            partsMap.merge(part, 1, (cnt, ignore) -> ++cnt);
        }

        assertEquals(CMDS_MAP.size(), partsMap.size());

        CMDS_MAP.forEach((p, set) -> {
            UpdateAllCommand cmd = (UpdateAllCommand) CollectionUtils.first(set);
            assertEquals(partsMap.get(p), cmd.rowsToUpdate().size(), () -> "part=" + p + ", set=" + set);

            cmd.rowsToUpdate().values().forEach(rowMessage -> {
                Row r = Row.wrapBinaryRow(schema, rowMessage.binaryRow());

                assertEquals(intTable.partition(r), p);
            });
        });
    }

    private void init(NativeTypeSpec t0, NativeTypeSpec t1) {
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

        tbl = new TableImpl(intTable, schemaRegistry, new HeapLockManager(), new ConstantSchemaVersions(1), mock(IgniteSql.class), -1);

        marshaller = new TupleMarshallerImpl(schema);
    }

    private Tuple createTuple(int k, NativeTypeSpec t0, NativeTypeSpec t1) {
        return Tuple.create()
                .set("ID", 1L)
                .set("ID0", generateValueByType(k, t0))
                .set("ID1", generateValueByType(k, t1))
                .set("VAL", 0L);
    }
}
