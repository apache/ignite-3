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
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.pendingChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.pendingPartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.plannedPartAssignmentsKey;
import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignmentForPartition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLongKeepingOrder;
import static org.apache.ignite.internal.util.ByteUtils.longToBytesKeepingOrder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for disaster recovery meta storage invoke that changes {@link RebalanceUtil#pendingChangeTriggerKey(TablePartitionId)}.
 */
public class DisasterRecoveryMsInvokeTest {
    private static final int partNum = 2;
    private static final int replicas = 2;

    private static final Set<String> nodes1 = IntStream.of(5).mapToObj(i -> "nodes1_" + i).collect(toSet());
    private static final Set<String> nodes2 = IntStream.of(5).mapToObj(i -> "nodes2_" + i).collect(toSet());
    private static final Set<String> nodes3 = IntStream.of(5).mapToObj(i -> "nodes3_" + i).collect(toSet());

    private static final Set<Assignment> assignments1 = calculateAssignmentForPartition(nodes1, partNum, replicas);
    private static final Set<Assignment> assignments2 = calculateAssignmentForPartition(nodes2, partNum, replicas);
    private static final Set<Assignment> assignments3 = calculateAssignmentForPartition(nodes3, partNum, replicas);

    private static final TablePartitionId tablePartitionId = new TablePartitionId(1, 1);

    private static final long expectedPendingChangeTriggerKey = 10L;

    private long assignmentsTimestamp;

    private final HybridClock clock = new HybridClockImpl();

    private MetaStorageManagerImpl metaStorageManager;

    @BeforeEach
    public void setUp() throws ExecutionException, InterruptedException {
        metaStorageManager = StandaloneMetaStorageManager.create();

        assertThat(metaStorageManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        metaStorageManager.deployWatches();

        metaStorageManager.put(pendingChangeTriggerKey(tablePartitionId), longToBytesKeepingOrder(1)).get();

        assignmentsTimestamp = clock.now().longValue();
    }

    @ParameterizedTest
    @MethodSource("assignments")
    public void testPendingChangeTriggerKey(
            Set<Assignment> currentPending,
            Set<Assignment> currentPlanned,
            Set<Assignment> pending,
            Set<Assignment> planned
    ) throws Exception {
        if (currentPending != null) {
            metaStorageManager.put(
                    pendingPartAssignmentsKey(tablePartitionId), Assignments.toBytes(currentPending, assignmentsTimestamp)
            ).get();
        }

        if (currentPlanned != null) {
            metaStorageManager.put(
                    plannedPartAssignmentsKey(tablePartitionId), Assignments.toBytes(currentPlanned, assignmentsTimestamp)
            ).get();
        }

        metaStorageManager.invoke(
                GroupUpdateRequest.prepareMsInvokeClosure(
                        tablePartitionId,
                        longToBytesKeepingOrder(expectedPendingChangeTriggerKey),
                        Assignments.toBytes(pending, assignmentsTimestamp),
                        Assignments.toBytes(planned, assignmentsTimestamp)
                )
        ).get();

        long actualPendingChangeTriggerKey = bytesToLongKeepingOrder(
                metaStorageManager.get(pendingChangeTriggerKey(tablePartitionId)).get().value()
        );

        assertEquals(expectedPendingChangeTriggerKey, actualPendingChangeTriggerKey);
    }

    private static Stream<Arguments> assignments() {
        return Stream.of(
                Arguments.of(null, null, assignments1, assignments2),
                Arguments.of(assignments1, null, assignments1, assignments2),
                Arguments.of(assignments1, null, assignments2, assignments3)
        );
    }
}
