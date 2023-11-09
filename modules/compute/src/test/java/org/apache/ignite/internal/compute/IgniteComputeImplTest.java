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

package org.apache.ignite.internal.compute;

import static java.util.Collections.singleton;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.utils.PrimaryReplica;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeImpl;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IgniteComputeImplTest extends BaseIgniteAbstractTest {
    @Mock
    private TopologyService topologyService;

    @Mock
    private IgniteTablesInternal igniteTables;

    @Mock
    private ComputeComponent computeComponent;

    @InjectMocks
    private IgniteComputeImpl compute;

    @Mock
    private TableViewInternal table;

    @Mock
    private InternalTable internalTable;

    private final ClusterNode localNode = new ClusterNodeImpl("local", "local", new NetworkAddress("local-host", 1));

    private final ClusterNode remoteNode = new ClusterNodeImpl("remote", "remote", new NetworkAddress("remote-host", 1));

    private final List<DeploymentUnit> testDeploymentUnits = List.of(new DeploymentUnit("test", "1.0.0"));

    @BeforeEach
    void setupMocks() {
        lenient().when(topologyService.localMember()).thenReturn(localNode);
        lenient().when(table.internalTable()).thenReturn(internalTable);
    }

    @Test
    void whenNodeIsLocalThenExecutesLocally() throws Exception {
        when(computeComponent.executeLocally(testDeploymentUnits, "org.example.SimpleJob", "a", 42))
                .thenReturn(CompletableFuture.completedFuture("jobResponse"));

        String result = compute.<String>executeAsync(singleton(localNode), testDeploymentUnits, "org.example.SimpleJob", "a", 42).get();

        assertThat(result, is("jobResponse"));

        verify(computeComponent).executeLocally(testDeploymentUnits, "org.example.SimpleJob", "a", 42);
    }

    @Test
    void whenNodeIsRemoteThenExecutesRemotely() throws Exception {
        respondWhenExecutingSimpleJobRemotely();

        String result = compute.<String>executeAsync(singleton(remoteNode), testDeploymentUnits, "org.example.SimpleJob", "a", 42).get();

        assertThat(result, is("remoteResponse"));

        verify(computeComponent).executeRemotely(remoteNode, testDeploymentUnits, "org.example.SimpleJob", "a", 42);
    }

    private void respondWhenExecutingSimpleJobRemotely() {
        when(computeComponent.executeRemotely(remoteNode, testDeploymentUnits, "org.example.SimpleJob", "a", 42))
                .thenReturn(CompletableFuture.completedFuture("remoteResponse"));
    }

    @Test
    void executesColocatedOnLeaderNodeOfPartitionCorrespondingToTupleKey() throws Exception {
        respondWhenExecutingSimpleJobRemotely();

        int partition = 42;

        when(igniteTables.tableViewAsync("TEST")).thenReturn(CompletableFuture.completedFuture(table));
        doReturn(partition).when(table).partition(any());
        doReturn(CompletableFuture.completedFuture(new PrimaryReplica(remoteNode, 0))).when(internalTable).primaryReplica(partition);

        String result = compute.<String>executeColocatedAsync(
                "test",
                Tuple.create(Map.of("k", 1)),
                testDeploymentUnits,
                "org.example.SimpleJob",
                "a", partition
        ).get();

        assertThat(result, is("remoteResponse"));
    }

    @Test
    void executesColocatedOnLeaderNodeOfPartitionCorrespondingToMappedKey() throws Exception {
        respondWhenExecutingSimpleJobRemotely();

        int partition = 42;

        when(igniteTables.tableViewAsync("TEST")).thenReturn(CompletableFuture.completedFuture(table));
        doReturn(partition).when(table).partition(any(), any());
        doReturn(CompletableFuture.completedFuture(new PrimaryReplica(remoteNode, 0))).when(internalTable).primaryReplica(partition);

        String result = compute.<Integer, String>executeColocatedAsync(
                "test",
                1,
                Mapper.of(Integer.class),
                testDeploymentUnits,
                "org.example.SimpleJob",
                "a", partition
        ).get();

        assertThat(result, is("remoteResponse"));
    }
}
