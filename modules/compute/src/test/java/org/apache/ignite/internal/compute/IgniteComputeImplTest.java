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

package org.apache.ignite.internal.compute;

import static java.util.Collections.singleton;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IgniteComputeImplTest {
    @Mock
    private TopologyService topologyService;

    @Mock
    private ComputeComponent computeComponent;

    @InjectMocks
    private IgniteComputeImpl compute;

    private final ClusterNode localNode = new ClusterNode("local", "local", new NetworkAddress("local-host", 1, "local"));
    private final ClusterNode remoteNode = new ClusterNode("remote", "remote", new NetworkAddress("remote-host", 1, "remote"));

    @BeforeEach
    void setupMocks() {
        lenient().when(topologyService.localMember()).thenReturn(localNode);
    }

    @Test
    void whenNodeIsLocalThenExecutesLocally() throws Exception {
        when(computeComponent.executeLocally(SimpleJob.class, "a", 42))
                .thenReturn(CompletableFuture.completedFuture("jobResponse"));

        String result = compute.execute(singleton(localNode), SimpleJob.class, "a", 42).get();

        assertThat(result, is("jobResponse"));

        verify(computeComponent).executeLocally(SimpleJob.class, "a", 42);
    }

    @Test
    void whenNodeIsRemoteThenExecutesRemotely() throws Exception {
        when(computeComponent.executeRemotely(remoteNode, SimpleJob.class, "a", 42))
                .thenReturn(CompletableFuture.completedFuture("remoteResponse"));

        String result = compute.execute(singleton(remoteNode), SimpleJob.class, "a", 42).get();

        assertThat(result, is("remoteResponse"));

        verify(computeComponent).executeRemotely(remoteNode, SimpleJob.class, "a", 42);
    }

    private static class SimpleJob implements ComputeJob<String> {
        /** {@inheritDoc} */
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            return "jobResponse";
        }
    }
}
