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
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
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
    private static final String JOB_CLASS_NAME = "org.example.SimpleJob";

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

    private final ClusterNode localNode = new ClusterNodeImpl("local", "local", new NetworkAddress("local-host", 1));

    private final ClusterNode remoteNode = new ClusterNodeImpl("remote", "remote", new NetworkAddress("remote-host", 1));

    private final List<DeploymentUnit> testDeploymentUnits = List.of(new DeploymentUnit("test", "1.0.0"));

    @BeforeEach
    void setupMocks() {
        lenient().when(topologyService.localMember()).thenReturn(localNode);
    }

    @Test
    void whenNodeIsLocalThenExecutesLocally() {
        respondWhenExecutingSimpleJobLocally();

        assertThat(
                compute.<String>executeAsync(singleton(localNode), testDeploymentUnits, JOB_CLASS_NAME, "a", 42).resultAsync(),
                willBe("jobResponse")
        );

        verify(computeComponent).executeLocally(testDeploymentUnits, JOB_CLASS_NAME, "a", 42);
    }

    @Test
    void whenNodeIsRemoteThenExecutesRemotely() {
        respondWhenExecutingSimpleJobRemotely();

        assertThat(
                compute.<String>executeAsync(singleton(remoteNode), testDeploymentUnits, JOB_CLASS_NAME, "a", 42).resultAsync(),
                willBe("remoteResponse")
        );

        verify(computeComponent).executeRemotely(remoteNode, testDeploymentUnits, JOB_CLASS_NAME, "a", 42);
    }

    @Test
    void executesColocatedOnLeaderNodeOfPartitionCorrespondingToTupleKey() {
        respondWhenExecutingSimpleJobRemotely();

        when(igniteTables.tableViewAsync("TEST")).thenReturn(completedFuture(table));
        doReturn(42).when(table).partition(any());
        doReturn(remoteNode).when(table).leaderAssignment(42);

        assertThat(
                compute.<String>executeColocatedAsync(
                        "test",
                        Tuple.create(Map.of("k", 1)),
                        testDeploymentUnits,
                        JOB_CLASS_NAME,
                        "a", 42
                ).resultAsync(),
                willBe("remoteResponse")
        );
    }

    @Test
    void executesColocatedOnLeaderNodeOfPartitionCorrespondingToMappedKey() {
        respondWhenExecutingSimpleJobRemotely();

        when(igniteTables.tableViewAsync("TEST")).thenReturn(completedFuture(table));
        doReturn(42).when(table).partition(any(), any());
        doReturn(remoteNode).when(table).leaderAssignment(42);

        assertThat(
                compute.<Integer, String>executeColocatedAsync(
                        "test",
                        1,
                        Mapper.of(Integer.class),
                        testDeploymentUnits,
                        JOB_CLASS_NAME,
                        "a", 42
                ).resultAsync(),
                willBe("remoteResponse")
        );
    }

    private void respondWhenExecutingSimpleJobLocally() {
        when(computeComponent.executeLocally(testDeploymentUnits, JOB_CLASS_NAME, "a", 42))
                .thenReturn(completedExecution("jobResponse"));
    }

    private void respondWhenExecutingSimpleJobRemotely() {
        when(computeComponent.executeRemotely(remoteNode, testDeploymentUnits, JOB_CLASS_NAME, "a", 42))
                .thenReturn(completedExecution("remoteResponse"));
    }

    private static <R> JobExecution<R> completedExecution(R result) {
        return new JobExecution<>() {
            @Override
            public CompletableFuture<R> resultAsync() {
                return completedFuture(result);
            }

            @Override
            public CompletableFuture<JobStatus> statusAsync() {
                return nullCompletedFuture();
            }

            @Override
            public CompletableFuture<Void> cancelAsync() {
                return nullCompletedFuture();
            }

            @Override
            public CompletableFuture<Void> changePriorityAsync(int newPriority) {
                return nullCompletedFuture();
            }
        };
    }
}
