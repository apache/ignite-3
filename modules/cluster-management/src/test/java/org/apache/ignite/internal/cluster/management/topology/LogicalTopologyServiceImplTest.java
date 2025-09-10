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

package org.apache.ignite.internal.cluster.management.topology;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class LogicalTopologyServiceImplTest extends BaseIgniteAbstractTest {
    @Mock
    private LogicalTopology logicalTopology;

    @Mock
    private ClusterManagementGroupManager cmgManager;

    @InjectMocks
    private LogicalTopologyServiceImpl logicalTopologyService;

    @Mock
    private LogicalTopologyEventListener listener;

    @Test
    void delegatesAddEventListener() {
        logicalTopologyService.addEventListener(listener);

        verify(logicalTopology).addEventListener(listener);
    }

    @Test
    void delegatesRemoveEventListener() {
        logicalTopologyService.removeEventListener(listener);

        verify(logicalTopology).removeEventListener(listener);
    }

    @Test
    void delegatesLogicalTopologyOnLeader() {
        CompletableFuture<LogicalTopologySnapshot> snapshotFuture = new CompletableFuture<>();

        doReturn(snapshotFuture).when(cmgManager).logicalTopology();

        assertThat(logicalTopologyService.logicalTopologyOnLeader(), is(snapshotFuture));
    }

    @Test
    void delegatesValidatedNodesOnLeader() {
        CompletableFuture<Set<InternalClusterNode>> snapshotFuture = new CompletableFuture<>();

        when(cmgManager.validatedNodes()).thenReturn(snapshotFuture);

        assertThat(logicalTopologyService.validatedNodesOnLeader(), is(snapshotFuture));
    }
}
