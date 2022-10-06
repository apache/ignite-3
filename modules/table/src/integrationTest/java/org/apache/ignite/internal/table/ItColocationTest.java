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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.hlc.HybridClock;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerException;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.table.distributed.command.UpdateAllCommand;
import org.apache.ignite.internal.table.distributed.command.UpdateCommand;
import org.apache.ignite.internal.table.distributed.command.response.MultiRowsResponse;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteMultiRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteSingleRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.storage.state.test.TestConcurrentHashMapTxStateTableStorage;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

/**
 * Tests for data colocation.
 */
public class ItColocationTest {
    /** Partitions count. */
    private static final int PARTS = 32;

    /** Keys count to check. */
    private static final int KEYS = 100;

    /** Dummy internal table for tests. */
    private static final InternalTable INT_TABLE;

    /** Map of the Raft commands are set by table operation. */
    private static final Int2ObjectMap<Set<Command>> CMDS_MAP = new Int2ObjectOpenHashMap<>();

    private SchemaDescriptor schema;

    private SchemaRegistry schemaRegistry;

    private TableImpl tbl;

    private TupleMarshallerImpl marshaller;

    static {
        ClusterService clusterService = Mockito.mock(ClusterService.class, RETURNS_DEEP_STUBS);
        when(clusterService.topologyService().localMember().address()).thenReturn(DummyInternalTableImpl.ADDR);

        ClusterNode clusterNode = new ClusterNode(UUID.randomUUID().toString(), "node", new NetworkAddress("", 0));

        ReplicaService replicaService = Mockito.mock(ReplicaService.class, RETURNS_DEEP_STUBS);

        TxManager txManager = new TxManagerImpl(replicaService,  new HeapLockManager(), new HybridClock()) {
            @Override
            public CompletableFuture<Void> finish(
                    ClusterNode recipientNode,
                    Long term,
                    boolean commit,
                    Map<ClusterNode, List<IgniteBiTuple<String, Long>>> groups,
                    UUID txId) {
                return completedFuture(null);
            }
        };
        txManager.start();

        Int2ObjectMap<RaftGroupService> partRafts = new Int2ObjectOpenHashMap<>();
        Map<String, RaftGroupService> groupRafts = new HashMap<>();

        for (int i = 0; i < PARTS; ++i) {
            String groupId = "PUBLIC.TEST_part_" + i;

            RaftGroupService r = Mockito.mock(RaftGroupService.class);
            when(r.leader()).thenReturn(Mockito.mock(Peer.class));
            when(r.groupId()).thenReturn(groupId);
            when(r.refreshAndGetLeaderWithTerm()).thenReturn(completedFuture(new IgniteBiTuple<>(new Peer(clusterNode.address()), 0L)));

            final int part = i;
            doAnswer(invocation -> {
                Command cmd = (Command) invocation.getArguments()[0];

                CMDS_MAP.merge(part, new HashSet<>(Set.of(cmd)), (newSet, set) -> {
                    set.addAll(newSet);

                    return set;
                });

                if (cmd instanceof UpdateAllCommand) {
                    return completedFuture(new MultiRowsResponse(List.of()).getValues());
                } else {
                    return completedFuture(true);
                }
            }).when(r).run(any());

            partRafts.put(i, r);
            groupRafts.put(groupId, r);
        }

        when(replicaService.invoke(any(), any())).thenAnswer(invocation -> {
            ReplicaRequest request = invocation.getArgument(1);

            RaftGroupService r = groupRafts.get(request.groupId());

            if (request instanceof ReadWriteMultiRowReplicaRequest) {
                Map<RowId, BinaryRow> rows = ((ReadWriteMultiRowReplicaRequest) request).binaryRows()
                        .stream()
                        .collect(toMap(row -> new RowId(0), row -> row));

                return r.run(new UpdateAllCommand(rows, UUID.randomUUID()));
            } else {
                assertThat(request, is(instanceOf(ReadWriteSingleRowReplicaRequest.class)));

                return r.run(new UpdateCommand(new RowId(0), ((ReadWriteSingleRowReplicaRequest) request).binaryRow(), UUID.randomUUID()));
            }
        });

        INT_TABLE = new InternalTableImpl(
                "PUBLIC.TEST",
                UUID.randomUUID(),
                partRafts,
                PARTS,
                null,
                address -> clusterNode,
                txManager,
                Mockito.mock(MvTableStorage.class),
                new TestConcurrentHashMapTxStateTableStorage(),
                replicaService,
                Mockito.mock(HybridClock.class)
        );
    }

    @BeforeEach
    public void beforeTest() {
        CMDS_MAP.clear();
    }

    private static NativeType nativeType(NativeTypeSpec type) {
        switch (type) {
            case INT8:
                return NativeTypes.INT8;
            case INT16:
                return NativeTypes.INT16;
            case INT32:
                return NativeTypes.INT32;
            case INT64:
                return NativeTypes.INT64;
            case FLOAT:
                return NativeTypes.FLOAT;
            case DOUBLE:
                return NativeTypes.DOUBLE;
            case DECIMAL:
                return NativeTypes.decimalOf(10, 3);
            case UUID:
                return NativeTypes.UUID;
            case STRING:
                return NativeTypes.STRING;
            case BYTES:
                return NativeTypes.BYTES;
            case BITMASK:
                return NativeTypes.bitmaskOf(16);
            case NUMBER:
                return NativeTypes.numberOf(10);
            case DATE:
                return NativeTypes.DATE;
            case TIME:
                return NativeTypes.time();
            case DATETIME:
                return NativeTypes.datetime();
            case TIMESTAMP:
                return NativeTypes.timestamp();
            default:
                throw new IllegalStateException("Unexpected type: " + type);
        }
    }

    private static Object generateValueByType(int i, NativeTypeSpec type) {
        switch (type) {
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
                        .atZone(TimeZone.getDefault().toZoneId())
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
    public void colocationTwoColumnsInsert(NativeTypeSpec t0, NativeTypeSpec t1)
            throws TupleMarshallerException {
        init(t0, t1);

        for (int i = 0; i < KEYS; ++i) {
            CMDS_MAP.clear();

            Tuple t = createTuple(i, t0, t1);

            tbl.recordView().insert(null, t);

            BinaryRowEx r = marshaller.marshal(t);

            int part = INT_TABLE.partition(r);

            assertThat(CollectionUtils.first(CMDS_MAP.get(part)), is(instanceOf(UpdateCommand.class)));
        }
    }

    /**
     * Check colocation by two columns for all types.
     */
    @ParameterizedTest(name = "types=" + ARGUMENTS_PLACEHOLDER)
    @MethodSource("twoColumnsParameters")
    public void colocationTwoColumnsInsertAll(NativeTypeSpec t0, NativeTypeSpec t1)
            throws TupleMarshallerException {
        init(t0, t1);

        tbl.recordView().insertAll(null, IntStream.range(0, KEYS).mapToObj(i -> createTuple(i, t0, t1)).collect(Collectors.toSet()));

        Int2IntMap partsMap = new Int2IntOpenHashMap();

        for (int i = 0; i < KEYS; ++i) {
            Tuple t = createTuple(i, t0, t1);

            BinaryRowEx r = marshaller.marshal(t);

            int part = INT_TABLE.partition(r);

            partsMap.merge(part, 1, (cnt, ignore) -> ++cnt);
        }

        assertEquals(partsMap.size(), CMDS_MAP.size());

        CMDS_MAP.forEach((p, set) -> {
            UpdateAllCommand cmd = (UpdateAllCommand) CollectionUtils.first(set);
            assertEquals(partsMap.get(p), cmd.getRowsToUpdate().size(), () -> "part=" + p + ", set=" + set);

            cmd.getRowsToUpdate().values().forEach(binRow -> {
                Row r = new Row(schema, binRow);

                assertEquals(INT_TABLE.partition(r), p);
            });
        });
    }

    private void init(NativeTypeSpec t0, NativeTypeSpec t1) {
        schema = new SchemaDescriptor(1,
                new Column[]{
                        new Column("ID", NativeTypes.INT64, false),
                        new Column("ID0", nativeType(t0), false),
                        new Column("ID1", nativeType(t1), false)
                },
                new String[]{"ID1", "ID0"},
                new Column[]{
                        new Column("VAL", NativeTypes.INT64, true)
                }
        );

        schemaRegistry = new DummySchemaManagerImpl(schema);

        tbl = new TableImpl(INT_TABLE, schemaRegistry);

        marshaller = new TupleMarshallerImpl(schemaRegistry);
    }

    private Tuple createTuple(int k, NativeTypeSpec t0, NativeTypeSpec t1) {
        return Tuple.create()
                .set("ID", 1L)
                .set("ID0", generateValueByType(k, t0))
                .set("ID1", generateValueByType(k, t1))
                .set("VAL", 0L);
    }
}
