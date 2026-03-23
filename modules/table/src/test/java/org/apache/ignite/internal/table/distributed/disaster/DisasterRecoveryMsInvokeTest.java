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

package org.apache.ignite.internal.table.distributed.disaster;

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.pendingChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.pendingPartAssignmentsQueueKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.plannedPartAssignmentsKey;
import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignmentForPartition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLongKeepingOrder;
import static org.apache.ignite.internal.util.ByteUtils.longToBytesKeepingOrder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.partitiondistribution.AssignmentsQueue;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for disaster recovery meta storage invoke that changes {@link ZoneRebalanceUtil#pendingChangeTriggerKey(ZonePartitionId)}.
 * We expect that the keys are changed in provided cases.
 */
public class DisasterRecoveryMsInvokeTest extends BaseIgniteAbstractTest {
    private static final int partNum = 2;
    private static final int replicas = 2;

    private static final Set<String> nodes1 = IntStream.of(5).mapToObj(i -> "nodes1_" + i).collect(toSet());
    private static final Set<String> nodes2 = IntStream.of(5).mapToObj(i -> "nodes2_" + i).collect(toSet());

    private static final Set<Assignment> assignments1 = calculateAssignmentForPartition(nodes1, partNum, partNum + 1, replicas, replicas);
    private static final Set<Assignment> assignments2 = calculateAssignmentForPartition(nodes2, partNum, partNum + 1, replicas, replicas);

    private static final ZonePartitionId zonePartitionId = new ZonePartitionId(1, 1);

    private static final HybridTimestamp expectedPendingChangeTimestampKey = HybridTimestamp.hybridTimestamp(1000L);

    private long assignmentsTimestamp;

    private final HybridClock clock = new HybridClockImpl();

    private MetaStorageManagerImpl metaStorageManager;

    @BeforeEach
    public void setUp() throws ExecutionException, InterruptedException {
        metaStorageManager = StandaloneMetaStorageManager.create();

        assertThat(metaStorageManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        metaStorageManager.deployWatches();

        assertThat(
                metaStorageManager.put(pendingChangeTriggerKey(zonePartitionId), longToBytesKeepingOrder(1)), willCompleteSuccessfully()
        );

        assignmentsTimestamp = clock.now().longValue();
    }

    @AfterEach
    public void cleanup() {
        assertThat(metaStorageManager.stopAsync(), willCompleteSuccessfully());
    }

    @ParameterizedTest
    @MethodSource("assignments")
    public void testPendingChangeTriggerKey(
            Set<Assignment> currentPending,
            Set<Assignment> pending
    ) throws Exception {
        if (currentPending != null) {
            assertThat(
                    metaStorageManager.put(
                            pendingPartAssignmentsQueueKey(zonePartitionId),
                            AssignmentsQueue.toBytes(Assignments.of(currentPending, assignmentsTimestamp))
                    ),
                    willCompleteSuccessfully()
            );
        }

        byte[] timestampBytes = longToBytesKeepingOrder(expectedPendingChangeTimestampKey.longValue());
        byte[] pendingAssignmentsBytes = AssignmentsQueue.toBytes(Assignments.of(pending, assignmentsTimestamp));
        ByteArray pendingChangeTriggerKey = pendingChangeTriggerKey(zonePartitionId);
        ByteArray partAssignmentsPendingKey = pendingPartAssignmentsQueueKey(zonePartitionId);
        ByteArray partAssignmentsPlannedKey = plannedPartAssignmentsKey(zonePartitionId);

        assertThat(
                metaStorageManager.invoke(
                        GroupUpdateRequestHandler.executeInvoke(
                                timestampBytes,
                                pendingAssignmentsBytes,
                                null,
                                pendingChangeTriggerKey,
                                partAssignmentsPendingKey,
                                partAssignmentsPlannedKey
                        )
                ),
                willCompleteSuccessfully()
        );

        CompletableFuture<Entry> actualPendingFut = metaStorageManager.get(pendingChangeTriggerKey(zonePartitionId));

        assertThat(actualPendingFut, willCompleteSuccessfully());

        HybridTimestamp actualTimestamp = HybridTimestamp.hybridTimestamp(bytesToLongKeepingOrder(actualPendingFut.get().value()));

        assertEquals(expectedPendingChangeTimestampKey, actualTimestamp);
    }

    private static Stream<Arguments> assignments() {
        return Stream.of(
                Arguments.of(null, assignments1),
                Arguments.of(assignments1, assignments1),
                Arguments.of(assignments1, assignments2)
        );
    }
}
