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
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.metastorage.server.KeyValueUpdateContext.kvContext;
import static org.apache.ignite.internal.partitiondistribution.Assignments.toBytes;
import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignmentForPartition;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLongKeepingOrder;
import static org.apache.ignite.internal.util.ByteUtils.longToBytesKeepingOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfileDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfilesDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.command.MetaStorageCommandsFactory;
import org.apache.ignite.internal.metastorage.command.MetaStorageWriteCommand;
import org.apache.ignite.internal.metastorage.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.impl.CommandIdGenerator;
import org.apache.ignite.internal.metastorage.server.KeyValueUpdateContext;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.partitiondistribution.AssignmentsQueue;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Tests for updating assignment in the meta storage.
 */
@ExtendWith({MockitoExtension.class, ConfigurationExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
public class ZoneRebalanceUtilUpdateAssignmentsTest extends IgniteAbstractTest {
    private static final IgniteLogger LOG = Loggers.forClass(ZoneRebalanceUtilUpdateAssignmentsTest.class);

    private static final KeyValueUpdateContext KV_UPDATE_CONTEXT = kvContext(HybridTimestamp.MIN_VALUE);

    private SimpleInMemoryKeyValueStorage keyValueStorage;

    @Mock
    private MetaStorageManager metaStorageManager;

    private final CatalogStorageProfileDescriptor storageProfileDescriptor = new CatalogStorageProfileDescriptor("default");

    private final CatalogStorageProfilesDescriptor storageProfilesDescriptor = new CatalogStorageProfilesDescriptor(
            List.of(storageProfileDescriptor)
    );

    private static final int partNum = 2;
    private static final int replicas = 2;

    private final CatalogZoneDescriptor zoneDescriptor = new CatalogZoneDescriptor(
            1,
            "zone1",
            partNum,
            replicas,
            replicas,
            1000,
            1000,
            "",
            storageProfilesDescriptor,
            ConsistencyMode.STRONG_CONSISTENCY
    );

    private final HybridClock clock = new HybridClockImpl();

    private static final Set<String> nodes1 = IntStream.of(5).mapToObj(i -> "nodes1_" + i).collect(toSet());
    private static final Set<String> nodes2 = IntStream.of(5).mapToObj(i -> "nodes2_" + i).collect(toSet());
    private static final Set<String> nodes3 = IntStream.of(5).mapToObj(i -> "nodes3_" + i).collect(toSet());
    private static final Set<String> nodes4 = IntStream.of(5).mapToObj(i -> "nodes4_" + i).collect(toSet());

    private static final Set<Assignment> assignments1 = calculateAssignmentForPartition(nodes1, partNum, partNum + 1, replicas, replicas);
    private static final Set<Assignment> assignments2 = calculateAssignmentForPartition(nodes2, partNum, partNum + 1, replicas, replicas);
    private static final Set<Assignment> assignments3 = calculateAssignmentForPartition(nodes3, partNum, partNum + 1, replicas, replicas);
    private static final Set<Assignment> assignments4 = calculateAssignmentForPartition(nodes4, partNum, partNum + 1, replicas, replicas);

    private static final long expectedPendingChangeTriggerKey = 10L;
    private static final HybridTimestamp expectedPendingChangeTimestampKey = hybridTimestamp(1000L);

    private long assignmentsTimestamp;

    @BeforeEach
    public void setUp() {
        ClusterService clusterService = mock(ClusterService.class);

        AtomicLong raftIndex = new AtomicLong();

        String nodeName = "test";

        keyValueStorage = spy(new SimpleInMemoryKeyValueStorage(nodeName));

        ClusterTimeImpl clusterTime = new ClusterTimeImpl(nodeName, new IgniteSpinBusyLock(), clock);

        MetaStorageListener metaStorageListener = new MetaStorageListener(keyValueStorage, clock, clusterTime);

        RaftGroupService metaStorageService = mock(RaftGroupService.class);

        // Delegate directly to listener.
        lenient().doAnswer(
                invocationClose -> {
                    Command cmd = invocationClose.getArgument(0);

                    long commandIndex = raftIndex.incrementAndGet();

                    if (cmd instanceof MetaStorageWriteCommand) {
                        ((MetaStorageWriteCommand) cmd).safeTime(hybridTimestamp(10));
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

        CommandIdGenerator commandIdGenerator = new CommandIdGenerator(UUID.randomUUID());

        lenient().doAnswer(invocationClose -> {
            Iif iif = invocationClose.getArgument(0);

            MultiInvokeCommand multiInvokeCommand = commandsFactory.multiInvokeCommand()
                    .iif(iif)
                    .id(commandIdGenerator.newId())
                    .initiatorTime(clock.now())
                    .build();

            return metaStorageService.run(multiInvokeCommand);
        }).when(metaStorageManager).invoke(any());

        when(clusterService.messagingService()).thenAnswer(invocation -> mock(MessagingService.class));

        assignmentsTimestamp = clock.now().longValue();
    }

    @AfterEach
    public void tearDown() {
        keyValueStorage.close();
    }

    private static Stream<Arguments> assignmentsProvider() {
        return Stream.of(
                arguments(nodes1, assignments2, null, null, null, null, assignments1, null),
                arguments(nodes1, assignments1, null, null, null, null, null, null),
                arguments(nodes1, assignments2, null, assignments3, null, null, assignments3, assignments1),
                arguments(nodes1, assignments1, null, assignments3, null, null, assignments3, assignments1),
                arguments(nodes1, assignments2, assignments3, null, null, assignments3, assignments1, null),
                arguments(nodes1, assignments1, assignments3, null, null, assignments3, assignments1, null),
                arguments(nodes1, assignments2, assignments1, null, null, assignments1, null, null),
                arguments(nodes1, assignments1, assignments1, null, null, assignments1, null, null),
                arguments(nodes1, assignments2, assignments2, null, null, assignments2, assignments1, null),
                arguments(nodes1, assignments2, assignments4, assignments3, null, assignments4, assignments3, assignments1),
                arguments(nodes1, assignments1, assignments3, assignments2, null, assignments3, assignments2, assignments1),
                arguments(nodes1, assignments2, assignments1, assignments3, null, assignments1, assignments3, assignments1),
                arguments(nodes1, assignments2, assignments2, assignments3, null, assignments2, assignments3, assignments1),
                arguments(nodes1, assignments1, assignments1, assignments2, null, assignments1, assignments2, assignments1),
                arguments(nodes1, assignments1, assignments1, assignments2, assignments3, assignments1, assignments2, assignments1),
                arguments(nodes1, assignments4, assignments1, assignments2, assignments1, assignments1, assignments2, assignments1),
                arguments(nodes2, assignments2, assignments1, assignments2, assignments1, assignments1, assignments2, null),
                arguments(nodes2, assignments4, assignments1, assignments2, assignments1, assignments1, assignments2, null)
        );
    }

    /**
     * Verifies that the metastorage has correct assignments after invoking {@link ZoneRebalanceUtil#updatePendingAssignmentsKeys}.
     * Uses {@link #assignmentsProvider()} as the parameter source.
     *
     * @param nodesForNewAssignments Nodes list to calculate new assignments against.
     * @param zoneCfgAssignments Zone's assignment set from the stable configuration.
     * @param currentStableAssignments Stable assignments already existing in the metastorage.
     * @param currentPendingAssignments Pending assignments already existing in the metastorage.
     * @param currentPlannedAssignments Planned assignments already existing in the metastorage.
     * @param expectedStableAssignments Stable assignments expected in the metastorage
     *        after invoking {@link ZoneRebalanceUtil#updatePendingAssignmentsKeys}.
     * @param expectedPendingAssignments Pending assignments expected in the metastorage
     *        after invoking {@link ZoneRebalanceUtil#updatePendingAssignmentsKeys}.
     * @param expectedPlannedAssignments Planned assignments expected in the metastorage
     *        after invoking {@link ZoneRebalanceUtil#updatePendingAssignmentsKeys}.
     */
    @DisplayName("Verify that assignments can be updated in metastorage")
    @MethodSource("assignmentsProvider")
    @ParameterizedTest(name = "[{index}] new nodes: {0}; stable configuration: {1}; assignments in metastorage: [{2}, {3}, {4}];"
            + " expected assignments after update: [{5}, {6}, {7}]")
    void testAssignmentsUpdate(
            Collection<String> nodesForNewAssignments,
            Set<Assignment> zoneCfgAssignments,
            Set<Assignment> currentStableAssignments,
            Set<Assignment> currentPendingAssignments,
            Set<Assignment> currentPlannedAssignments,
            Set<Assignment> expectedStableAssignments,
            Set<Assignment> expectedPendingAssignments,
            Set<Assignment> expectedPlannedAssignments
    ) {
        ZonePartitionId zonePartitionId = new ZonePartitionId(1, 1);

        if (currentStableAssignments != null) {
            keyValueStorage.put(
                    ZoneRebalanceUtil.stablePartAssignmentsKey(zonePartitionId).bytes(),
                    toBytes(currentStableAssignments, assignmentsTimestamp),
                    KV_UPDATE_CONTEXT
            );
        }

        if (currentPendingAssignments != null) {
            keyValueStorage.put(
                    ZoneRebalanceUtil.pendingPartAssignmentsQueueKey(zonePartitionId).bytes(),
                    AssignmentsQueue.toBytes(Assignments.of(currentPendingAssignments, assignmentsTimestamp)),
                    KV_UPDATE_CONTEXT
            );
        }

        if (currentPlannedAssignments != null) {
            keyValueStorage.put(
                    ZoneRebalanceUtil.plannedPartAssignmentsKey(zonePartitionId).bytes(),
                    toBytes(currentPlannedAssignments, assignmentsTimestamp),
                    KV_UPDATE_CONTEXT
            );
        }

        keyValueStorage.put(
                ZoneRebalanceUtil.pendingChangeTriggerKey(zonePartitionId).bytes(),
                longToBytesKeepingOrder(1),
                KV_UPDATE_CONTEXT
        );

        ZoneRebalanceUtil.updatePendingAssignmentsKeys(
                zoneDescriptor,
                zonePartitionId,
                nodesForNewAssignments,
                partNum + 1,
                replicas,
                replicas,
                expectedPendingChangeTriggerKey,
                expectedPendingChangeTimestampKey,
                metaStorageManager,
                partNum,
                zoneCfgAssignments,
                assignmentsTimestamp,
                Set.of(),
                ConsistencyMode.STRONG_CONSISTENCY
        );

        byte[] actualStableBytes = keyValueStorage.get(ZoneRebalanceUtil.stablePartAssignmentsKey(zonePartitionId).bytes()).value();
        Set<Assignment> actualStableAssignments = null;

        if (actualStableBytes != null) {
            actualStableAssignments = Assignments.fromBytes(actualStableBytes).nodes();
        }

        byte[] actualPendingBytes = keyValueStorage.get(ZoneRebalanceUtil.pendingPartAssignmentsQueueKey(zonePartitionId).bytes()).value();
        Set<Assignment> actualPendingAssignments = null;

        if (actualPendingBytes != null) {
            actualPendingAssignments = AssignmentsQueue.fromBytes(actualPendingBytes).poll().nodes();
        }

        byte[] actualPlannedBytes = keyValueStorage.get(ZoneRebalanceUtil.plannedPartAssignmentsKey(zonePartitionId).bytes()).value();
        Set<Assignment> actualPlannedAssignments = null;

        if (actualPlannedBytes != null) {
            actualPlannedAssignments = Assignments.fromBytes(actualPlannedBytes).nodes();
        }

        byte[] pendingChangeTriggerKey = keyValueStorage.get(ZoneRebalanceUtil.pendingChangeTriggerKey(zonePartitionId).bytes()).value();
        HybridTimestamp actualPendingChangeTimestamp = hybridTimestamp(bytesToLongKeepingOrder(pendingChangeTriggerKey));

        LOG.info("stableAssignments {}", actualStableAssignments);
        LOG.info("pendingAssignments {}", actualPendingAssignments);
        LOG.info("plannedAssignments {}", actualPlannedAssignments);

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

        assertEquals(expectedPendingChangeTimestampKey, actualPendingChangeTimestamp);
    }
}
