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

package org.apache.ignite.internal.index;

import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.mockito.Mockito.mock;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.junit.jupiter.api.Test;

/** For {@link NodeLeftLogicalTopologyListener} testing. */
public class NodeLeftLogicalTopologyListenerTest extends IgniteAbstractTest {
    @Test
    void testOnNodeLeft() {
        LogicalNode logicalNode = mock(LogicalNode.class);

        var remainingNodes = new HashSet<>(Set.of(logicalNode));

        var listener = new NodeLeftLogicalTopologyListener(remainingNodes);

        listener.onNodeLeft(first(remainingNodes), mock(LogicalTopologySnapshot.class));

        assertThat(remainingNodes, empty());
    }

    @Test
    void testOnNodeValidated() {
        LogicalNode logicalNode = mock(LogicalNode.class);

        var remainingNodes = new HashSet<>(Set.of(logicalNode));

        var listener = new NodeLeftLogicalTopologyListener(remainingNodes);

        listener.onNodeValidated(first(remainingNodes));

        assertThat(remainingNodes, contains(logicalNode));
    }

    @Test
    void testOnNodeJoined() {
        LogicalNode logicalNode = mock(LogicalNode.class);

        var remainingNodes = new HashSet<>(Set.of(logicalNode));

        var listener = new NodeLeftLogicalTopologyListener(remainingNodes);

        listener.onNodeJoined(first(remainingNodes), mock(LogicalTopologySnapshot.class));

        assertThat(remainingNodes, contains(logicalNode));
    }

    @Test
    void testOnTopologyLeap() {
        LogicalNode logicalNode = mock(LogicalNode.class);

        var remainingNodes = new HashSet<>(Set.of(logicalNode));

        var listener = new NodeLeftLogicalTopologyListener(remainingNodes);

        listener.onTopologyLeap(mock(LogicalTopologySnapshot.class));

        assertThat(remainingNodes, contains(logicalNode));
    }
}
