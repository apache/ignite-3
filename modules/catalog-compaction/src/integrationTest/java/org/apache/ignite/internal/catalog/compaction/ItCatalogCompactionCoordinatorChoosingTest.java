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

package org.apache.ignite.internal.catalog.compaction;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.hamcrest.CustomMatcher;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests to verify choosing of coordinator for catalog compaction.
 */
@Disabled("https://github.com/linkedin/ignite-3/issues - TODO: Cluster initialization fails on Java 11")
class ItCatalogCompactionCoordinatorChoosingTest extends ClusterPerClassIntegrationTest {
    private static final int CLUSTER_SIZE = 5;
    private static final int MINORITY_SIZE = (CLUSTER_SIZE - 1) / 2;

    // test is sensible to count of nodes in cluster
    @Override
    protected int initialNodes() {
        return CLUSTER_SIZE;
    }

    @Override
    protected int[] cmgMetastoreNodes() {
        return IntStream.range(0, CLUSTER_SIZE).toArray();
    }

    @Test
    void coordinatorIsChosenAfterStartAndReelectedAfterNodeCrash() throws Exception {
        Set<String> coordinators = new HashSet<>();

        int coordinatorHasNotBeenUpdatedCount = 0;
        for (IgniteImpl node : runningNodes()) {
            String coordinatorNodeId = node.catalogCompactionRunner().coordinator();

            if (coordinatorNodeId == null) {
                coordinatorHasNotBeenUpdatedCount++;
            } else {
                coordinators.add(coordinatorNodeId);
            }
        }

        assertThat(coordinatorHasNotBeenUpdatedCount, lessOrEqualTo(MINORITY_SIZE));
        assertThat(coordinators, hasSize(1));

        String coordinatorNodeName = coordinators.iterator().next();

        // let's kill current coordinator node and wait until coordinator will be updated on all remaining nodes
        CLUSTER.stopNode(coordinatorNodeName);

        assertTrue(waitForCondition(() -> {
            coordinators.clear();

            for (IgniteImpl node : runningNodes()) {
                String nodeViewCoordinatorNodeName = node.catalogCompactionRunner().coordinator();

                if (nodeViewCoordinatorNodeName == null || coordinatorNodeName.equals(nodeViewCoordinatorNodeName)) {
                    return false;
                }

                coordinators.add(nodeViewCoordinatorNodeName);
            }

            return true;
        }, 10_000));

        // all node should have consistent view
        assertThat(coordinators, hasSize(1));
    }

    private static List<IgniteImpl> runningNodes() {
        return CLUSTER.runningNodes().map(TestWrappers::unwrapIgniteImpl).collect(Collectors.toList());
    }

    private static <T extends Comparable<T>> Matcher<T> lessOrEqualTo(T value) {
        return new CustomMatcher<>("value that less or equal to " + value) {
            @Override
            public boolean matches(Object o) {
                return o instanceof Comparable && ((Comparable<T>) o).compareTo(value) <= 0;
            }
        };
    }

    private static Matcher<Collection<?>> hasSize(int size) {
        return new CustomMatcher<>("collection of size=" + size) {
            @Override
            public boolean matches(Object o) {
                return o instanceof Collection && ((Collection<?>) o).size() == size;
            }
        };
    }
}
