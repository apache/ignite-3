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

package org.apache.ignite.internal.placementdriver;

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.table.TableImpl;
import org.junit.jupiter.api.Test;

/**
 * Tests that placement driver components are operable on different cluster nodes.
 */
public class PlacementDriverNodesOperabilityTest extends ClusterPerClassIntegrationTest {
    @Override
    protected int initialNodes() {
        return 2;
    }

    /**
     * Tests that assignments are tracked on every node.
     */
    @Test
    public void testAssignmentsAreTrackedOnEveryNode() throws InterruptedException {
        sql("CREATE ZONE IF NOT EXISTS ZONE_TEST (REPLICAS 2, PARTITIONS 1) STORAGE PROFILES ['default']");
        sql("CREATE TABLE TABLE_TEST (id INT PRIMARY KEY, name VARCHAR) ZONE ZONE_TEST");

        Set<Integer> cmgMetastoreNodesIndices = Arrays.stream(cmgMetastoreNodes()).boxed().collect(toSet());

        IgniteImpl metaStorageNode = unwrapIgniteImpl(findAliveNode(ni -> cmgMetastoreNodesIndices.contains(ni.getKey())));

        IgniteImpl nonMetaStorageNode = unwrapIgniteImpl(findAliveNode(ni -> !cmgMetastoreNodesIndices.contains(ni.getKey())));

        TableImpl table = unwrapTableImpl(metaStorageNode.tables().table("TABLE_TEST"));
        ZonePartitionId groupId = new ZonePartitionId(table.internalTable().zoneId(), 0);

        assertTrue(waitForCondition(() -> nonNullNonEmptyNoNullElements(assignments(metaStorageNode, groupId)), 3000));
        assertTrue(waitForCondition(() -> nonNullNonEmptyNoNullElements(assignments(nonMetaStorageNode, groupId)), 3000));
    }

    private Ignite findAliveNode(Predicate<IgniteBiTuple<Integer, Ignite>> predicate) {
        return aliveNodesWithIndices().stream()
                .filter(predicate)
                .map(IgniteBiTuple::getValue)
                .findFirst()
                .orElseThrow();
    }

    private List<TokenizedAssignments> assignments(IgniteImpl node, ZonePartitionId groupId) {
        return node.placementDriver().getAssignments(List.of(groupId), node.clock().now()).join();
    }

    private static boolean nonNullNonEmptyNoNullElements(List<?> list) {
        return list != null && !list.isEmpty() && list.stream().noneMatch(Objects::isNull);
    }
}
