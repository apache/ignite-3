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

package org.apache.ignite.internal.distributionzones.rebalance;

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.affinity.AffinityUtils.calculateAssignmentForPartition;
import static org.apache.ignite.internal.catalog.CatalogManagerImpl.INITIAL_CAUSALITY_TOKEN;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.command.MetaStorageCommandsFactory;
import org.apache.ignite.internal.metastorage.command.MetaStorageWriteCommand;
import org.apache.ignite.internal.metastorage.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Tests for updating assignment in the meta storage.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class RebalanceUtilUpdateAssignmentsTest extends IgniteAbstractTest {
    private static final IgniteLogger LOG = Loggers.forClass(RebalanceUtilUpdateAssignmentsTest.class);

    private SimpleInMemoryKeyValueStorage keyValueStorage;

    private ClusterService clusterService;

    private MetaStorageManager metaStorageManager;

    private final CatalogTableDescriptor tableDescriptor = new CatalogTableDescriptor(
            1,
            -1,
            "table1",
            0,
            1,
            List.of(new CatalogTableColumnDescriptor("k1", ColumnType.INT32, false, 0, 0, 0, null)),
            List.of("k1"),
            null,
            INITIAL_CAUSALITY_TOKEN
    );

    private static final int partNum = 2;
    private static final int replicas = 2;

    private static final Set<String> nodes1 = IntStream.of(5).mapToObj(i -> "nodes1_" + i).collect(toSet());
    private static final Set<String> nodes2 = IntStream.of(5).mapToObj(i -> "nodes2_" + i).collect(toSet());
    private static final Set<String> nodes3 = IntStream.of(5).mapToObj(i -> "nodes3_" + i).collect(toSet());
    private static final Set<String> nodes4 = IntStream.of(5).mapToObj(i -> "nodes4_" + i).collect(toSet());

    private static final Set<Assignment> assignments1 = calculateAssignmentForPartition(nodes1, partNum, replicas);
    private static final Set<Assignment> assignments2 = calculateAssignmentForPartition(nodes2, partNum, replicas);
    private static final Set<Assignment> assignments3 = calculateAssignmentForPartition(nodes3, partNum, replicas);
    private static final Set<Assignment> assignments4 = calculateAssignmentForPartition(nodes4, partNum, replicas);

    @BeforeEach
    public void setUp() {
        clusterService = mock(ClusterService.class);

        metaStorageManager = mock(MetaStorageManager.class);

        AtomicLong raftIndex = new AtomicLong();

        keyValueStorage = spy(new SimpleInMemoryKeyValueStorage("test"));

        MetaStorageListener metaStorageListener = new MetaStorageListener(keyValueStorage, mock(ClusterTimeImpl.class));

        RaftGroupService metaStorageService = mock(RaftGroupService.class);

        // Delegate directly to listener.
        lenient().doAnswer(
                invocationClose -> {
                    Command cmd = invocationClose.getArgument(0);

                    long commandIndex = raftIndex.incrementAndGet();

                    if (cmd instanceof MetaStorageWriteCommand) {
                        ((MetaStorageWriteCommand) cmd).safeTimeLong(10);
                    }

                    CompletableFuture<Serializable> res = new CompletableFuture<>();

                    CommandClosure<WriteCommand> clo = new CommandClosure<>() {
                        @Override
                        public long index() {
                            return commandIndex;
                        }

                        @Override
                        public WriteCommand command() {
                            return (WriteCommand) cmd;
                        }

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
                        metaStorageListener.onWrite(List.of(clo).iterator());
                    } catch (Throwable e) {
                        res.completeExceptionally(new IgniteInternalException(e));
                    }

                    return res;
                }
        ).when(metaStorageService).run(any());

        MetaStorageCommandsFactory commandsFactory = new MetaStorageCommandsFactory();

        lenient().doAnswer(invocationClose -> {
            Iif iif = invocationClose.getArgument(0);

            MultiInvokeCommand multiInvokeCommand = commandsFactory.multiInvokeCommand().iif(iif).build();

            return metaStorageService.run(multiInvokeCommand);
        }).when(metaStorageManager).invoke(any());

        when(clusterService.messagingService()).thenAnswer(invocation -> {
            MessagingService ret = mock(MessagingService.class);

            return ret;
        });
    }

    @AfterEach
    public void tearDown() {
        keyValueStorage.close();
    }

    /**
     * Nodes for new assignments calculating: nodes1.
     * The table configuration assignments: assignments2.
     * Current assignments in the metastorage: stable=null, pending=null, planned=null.
     * Expected assignments in the metastorage after updating: stable=null, pending=assignments1, planned=null.
     */
    @Test
    void test1() {
        test(
                nodes1, assignments2,
                null, null, null,
                null, assignments1, null
        );
    }

    /**
     * Nodes for new assignments calculating: nodes1.
     * The table configuration assignments: assignments1.
     * Current assignments in the metastorage: stable=null, pending=null, planned=null.
     * Expected assignments in the metastorage after updating: stable=null, pending=null, planned=null.
     */
    @Test
    void test2() {
        test(
                nodes1, assignments1,
                null, null, null,
                null, null, null
        );
    }

    /**
     * Nodes for new assignments calculating: nodes1.
     * The table configuration assignments: assignments2.
     * Current assignments in the metastorage: stable=null, pending=assignments3, planned=null.
     * Expected assignments in the metastorage after updating: stable=null, pending=assignments3, planned=assignments1.
     */
    @Test
    void test3() {
        test(
                nodes1, assignments2,
                null, assignments3, null,
                null, assignments3, assignments1
        );
    }

    /**
     * Nodes for new assignments calculating: nodes1.
     * The table configuration assignments: assignments1.
     * Current assignments in the metastorage: stable=null, pending=assignments3, planned=null.
     * Expected assignments in the metastorage after updating: stable=null, pending=assignments3, planned=assignments1.
     */
    @Test
    void test4() {
        test(
                nodes1, assignments1,
                null, assignments3, null,
                null, assignments3, assignments1
        );
    }

    /**
     * Nodes for new assignments calculating: nodes1.
     * The table configuration assignments: assignments2.
     * Current assignments in the metastorage: stable=assignments3, pending=null, planned=null.
     * Expected assignments in the metastorage after updating: stable=assignments3, pending=assignments1, planned=null.
     */
    @Test
    void test5() {
        test(
                nodes1, assignments2,
                assignments3, null, null,
                assignments3, assignments1, null
        );
    }

    /**
     * Nodes for new assignments calculating: nodes1.
     * The table configuration assignments: assignments1.
     * Current assignments in the metastorage: stable=assignments3, pending=null, planned=null.
     * Expected assignments in the metastorage after updating: stable=assignments3, pending=assignments1, planned=null.
     */
    @Test
    void test6() {
        test(
                nodes1, assignments1,
                assignments3, null, null,
                assignments3, assignments1, null
        );
    }

    /**
     * Nodes for new assignments calculating: nodes1.
     * The table configuration assignments: assignments2.
     * Current assignments in the metastorage: stable=assignments1, pending=null, planned=null.
     * Expected assignments in the metastorage after updating: stable=assignments1, pending=null, planned=null.
     */
    @Test
    void test7() {
        test(
                nodes1, assignments2,
                assignments1, null, null,
                assignments1, null, null
        );
    }

    /**
     * Nodes for new assignments calculating: nodes1.
     * The table configuration assignments: assignments1.
     * Current assignments in the metastorage: stable=assignments1, pending=null, planned=null.
     * Expected assignments in the metastorage after updating: stable=assignments1, pending=null, planned=null.
     */
    @Test
    void test8() {
        test(
                nodes1, assignments1,
                assignments1, null, null,
                assignments1, null, null
        );
    }

    /**
     * Nodes for new assignments calculating: nodes1.
     * The table configuration assignments: assignments2.
     * Current assignments in the metastorage: stable=assignments2, pending=null, planned=null.
     * Expected assignments in the metastorage after updating: stable=assignments2, pending=assignments1, planned=null.
     */
    @Test
    void test9() {
        test(
                nodes1, assignments2,
                assignments2, null, null,
                assignments2, assignments1, null
        );
    }

    /**
     * Nodes for new assignments calculating: nodes1.
     * The table configuration assignments: assignments2.
     * Current assignments in the metastorage: stable=assignments4, pending=assignments3, planned=null.
     * Expected assignments in the metastorage after updating: stable=assignments4, pending=assignments3, planned=assignments1.
     */
    @Test
    void test10() {
        test(
                nodes1, assignments2,
                assignments4, assignments3, null,
                assignments4, assignments3, assignments1
        );
    }

    /**
     * Nodes for new assignments calculating: nodes1.
     * The table configuration assignments: assignments1.
     * Current assignments in the metastorage: stable=assignments3, pending=assignments2, planned=null.
     * Expected assignments in the metastorage after updating: stable=assignments3, pending=assignments2, planned=assignments1.
     */
    @Test
    void test11() {
        test(
                nodes1, assignments1,
                assignments3, assignments2, null,
                assignments3, assignments2, assignments1
        );
    }

    /**
     * Nodes for new assignments calculating: nodes1.
     * The table configuration assignments: assignments2.
     * Current assignments in the metastorage: stable=assignments1, pending=assignments3, planned=null.
     * Expected assignments in the metastorage after updating: stable=assignments1, pending=assignments3, planned=assignments1.
     */
    @Test
    void test12() {
        test(
                nodes1, assignments2,
                assignments1, assignments3, null,
                assignments1, assignments3, assignments1
        );
    }

    /**
     * Nodes for new assignments calculating: nodes1.
     * The table configuration assignments: assignments2.
     * Current assignments in the metastorage: stable=assignments2, pending=assignments3, planned=null.
     * Expected assignments in the metastorage after updating: stable=assignments2, pending=assignments3, planned=assignments1.
     */
    @Test
    void test13() {
        test(
                nodes1, assignments2,
                assignments2, assignments3, null,
                assignments2, assignments3, assignments1
        );
    }

    /**
     * Nodes for new assignments calculating: nodes1.
     * The table configuration assignments: assignments1.
     * Current assignments in the metastorage: stable=assignments1, pending=assignments2, planned=null.
     * Expected assignments in the metastorage after updating: stable=assignments1, pending=assignments2, planned=assignments1.
     */
    @Test
    void test14() {
        test(
                nodes1, assignments1,
                assignments1, assignments2, null,
                assignments1, assignments2, assignments1
        );
    }

    /**
     * Nodes for new assignments calculating: nodes1.
     * The table configuration assignments: assignments1.
     * Current assignments in the metastorage: stable=assignments1, pending=assignments2, planned=assignments3.
     * Expected assignments in the metastorage after updating: stable=assignments1, pending=assignments2, planned=assignments1.
     */
    @Test
    void test15() {
        test(
                nodes1, assignments1,
                assignments1, assignments2, assignments3,
                assignments1, assignments2, assignments1
        );
    }

    /**
     * Nodes for new assignments calculating: nodes1.
     * The table configuration assignments: assignments4.
     * Current assignments in the metastorage: stable=assignments1, pending=assignments2, planned=assignments1.
     * Expected assignments in the metastorage after updating: stable=assignments1, pending=assignments2, planned=assignments1.
     */
    @Test
    void test16() {
        test(
                nodes1, assignments4,
                assignments1, assignments2, assignments1,
                assignments1, assignments2, assignments1
        );
    }

    /**
     * Nodes for new assignments calculating: nodes2.
     * The table configuration assignments: assignments2.
     * Current assignments in the metastorage: stable=assignments1, pending=assignments2, planned=assignments1.
     * Expected assignments in the metastorage after updating: stable=assignments1, pending=assignments2, planned=null.
     */
    @Test
    void test17() {
        test(
                nodes2, assignments2,
                assignments1, assignments2, assignments1,
                assignments1, assignments2, null
        );
    }

    /**
     * Nodes for new assignments calculating: nodes2.
     * The table configuration assignments: assignments4.
     * Current assignments in the metastorage: stable=assignments1, pending=assignments2, planned=assignments1.
     * Expected assignments in the metastorage after updating: stable=assignments1, pending=assignments2, planned=null.
     */
    @Test
    void test18() {
        test(
                nodes2, assignments4,
                assignments1, assignments2, assignments1,
                assignments1, assignments2, null
        );
    }

    private void test(
            Collection<String> nodesForNewAssignments,
            Set<Assignment> tableCfgAssignments,
            Set<Assignment> currentStableAssignments,
            Set<Assignment> currentPendingAssignments,
            Set<Assignment> currentPlannedAssignments,
            Set<Assignment> expectedStableAssignments,
            Set<Assignment> expectedPendingAssignments,
            Set<Assignment> expectedPlannedAssignments
    ) {
        TablePartitionId tablePartitionId = new TablePartitionId(1, 1);

        if (currentStableAssignments != null) {
            keyValueStorage.put(RebalanceUtil.stablePartAssignmentsKey(tablePartitionId).bytes(), toBytes(currentStableAssignments),
                    HybridTimestamp.MIN_VALUE);
        }

        if (currentPendingAssignments != null) {
            keyValueStorage.put(RebalanceUtil.pendingPartAssignmentsKey(tablePartitionId).bytes(), toBytes(currentPendingAssignments),
                    HybridTimestamp.MIN_VALUE);
        }

        if (currentPlannedAssignments != null) {
            keyValueStorage.put(RebalanceUtil.plannedPartAssignmentsKey(tablePartitionId).bytes(), toBytes(currentPlannedAssignments),
                    HybridTimestamp.MIN_VALUE);
        }

        RebalanceUtil.updatePendingAssignmentsKeys(
                tableDescriptor, tablePartitionId, nodesForNewAssignments,
                replicas, 1, metaStorageManager, partNum, tableCfgAssignments
        );

        byte[] actualStableBytes = keyValueStorage.get(RebalanceUtil.stablePartAssignmentsKey(tablePartitionId).bytes()).value();
        Set<Assignment> actualStableAssignments = null;

        if (actualStableBytes != null) {
            actualStableAssignments = ByteUtils.fromBytes(actualStableBytes);
        }

        byte[] actualPendingBytes = keyValueStorage.get(RebalanceUtil.pendingPartAssignmentsKey(tablePartitionId).bytes()).value();
        Set<Assignment> actualPendingAssignments = null;

        if (actualPendingBytes != null) {
            actualPendingAssignments = ByteUtils.fromBytes(actualPendingBytes);
        }

        byte[] actualPlannedBytes = keyValueStorage.get(RebalanceUtil.plannedPartAssignmentsKey(tablePartitionId).bytes()).value();
        Set<Assignment> actualPlannedAssignments = null;

        if (actualPlannedBytes != null) {
            actualPlannedAssignments = ByteUtils.fromBytes(actualPlannedBytes);
        }

        LOG.info("stableAssignments " + actualStableAssignments);
        LOG.info("pendingAssignments " + actualPendingAssignments);
        LOG.info("plannedAssignments " + actualPlannedAssignments);

        if (expectedStableAssignments != null) {
            assertNotNull(actualStableBytes);
            assertEquals(actualStableAssignments, expectedStableAssignments);
        } else {
            assertNull(actualStableBytes);
        }

        if (expectedPendingAssignments != null) {
            assertNotNull(actualPendingBytes);
            assertEquals(actualPendingAssignments, expectedPendingAssignments);
        } else {
            assertNull(actualPendingBytes);
        }

        if (expectedPlannedAssignments != null) {
            assertNotNull(actualPlannedBytes);
            assertEquals(actualPlannedAssignments, expectedPlannedAssignments);
        } else {
            assertNull(actualPlannedBytes);
        }
    }
}
