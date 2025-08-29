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

package org.apache.ignite.internal.network;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.network.ClusterNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tests for {@link org.apache.ignite.network.IgniteCluster} API.
 */
public abstract class ItAbstractIgniteClusterTest extends ClusterPerClassIntegrationTest {
    @Override
    protected int initialNodes() {
        return 2;
    }

    protected abstract Ignite ignite();

    protected boolean hasNodeMeta() {
        return true;
    }

    private String nodeNamePrefix() {
        return IgniteTestUtils.shortTestMethodName(this.getClass().getSimpleName()) + "_n_";
    }

    @SuppressWarnings({"DataFlowIssue", "deprecation", "NestedConditionalExpression"})
    @ParameterizedTest
    @CsvSource({
        "true, true",
        "true, false",
        "false, true",
        "false, false"
    })
    public void testNodes(boolean deprecated, boolean async) {
        Collection<ClusterNode> nodes = deprecated
                ? (async ? ignite().clusterNodesAsync().join() : ignite().clusterNodes())
                : (async ? ignite().cluster().nodesAsync().join() : ignite().cluster().nodes());

        assertEquals(2, nodes.size());

        List<ClusterNode> nodes0 = nodes.stream().sorted(Comparator.comparing(ClusterNode::name)).collect(Collectors.toList());

        assertEquals(nodeNamePrefix() + "3344", nodes0.get(0).name());
        assertEquals(nodeNamePrefix() + "3345", nodes0.get(1).name());

        assertEquals(3344, nodes0.get(0).address().port());
        assertEquals(3345, nodes0.get(1).address().port());

        if (hasNodeMeta()) {
            assertNotNull(nodes0.get(0).nodeMetadata());
            assertNotNull(nodes0.get(1).nodeMetadata());

            assertEquals(10300, nodes0.get(0).nodeMetadata().httpPort());
            assertEquals(10301, nodes0.get(1).nodeMetadata().httpPort());
        } else {
            assertNull(nodes0.get(0).nodeMetadata());
            assertNull(nodes0.get(1).nodeMetadata());
        }
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    public void testLocalNode() {
        ClusterNode localNode = ignite().cluster().localNode();
        assertNotNull(localNode);

        assertEquals(nodeNamePrefix() + "3344", localNode.name());
        assertEquals(3344, localNode.address().port());

        if (hasNodeMeta()) {
            assertNotNull(localNode.nodeMetadata());
            assertEquals(10300, localNode.nodeMetadata().httpPort());
        } else {
            assertNull(localNode.nodeMetadata());
        }
    }

    @Test
    void testClusterId() {
        UUID expected = unwrapIgniteImpl(node(0)).clusterIdService().clusterId();
        assertEquals(expected, ignite().cluster().id());
    }
}
