/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.affinity;

import java.util.Arrays;
import java.util.UUID;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests scenarios for affinity manager.
 */
public class AffinityManagerTest {
    // TODO sanpwc: Properly write tests for affinity manager.
    @Test
    public void testCalculatedAssignment() {
        BaselineManager baselineMgrMock = mock(BaselineManager.class);

        when(baselineMgrMock.nodes()).thenReturn(
            Arrays.asList(
                new ClusterNode(
                    UUID.randomUUID().toString(), "node0",
                    new NetworkAddress("localhost", 8080)
                ),
                new ClusterNode(
                    UUID.randomUUID().toString(), "node1",
                    new NetworkAddress("localhost", 8081)
                )

            )
        );

        AffinityManager affinityMgr = new AffinityManager(baselineMgrMock);

        affinityMgr.calculateAssignments(10, 3);
    }
}
