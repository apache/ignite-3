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

package org.apache.ignite.raft.jraft.rpc;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.raft.jraft.test.TestUtils.INIT_PORT;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcClient;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInfo;

/**
 * Ignite RPC test.
 */
public class IgniteRpcTest extends AbstractRpcTest {
    private static final IgniteLogger LOG = Loggers.forClass(IgniteRpcTest.class);

    /** The counter. */
    private final AtomicInteger cntr = new AtomicInteger();

    /** Requests executor. */
    private ExecutorService requestExecutor;

    /** Test info. */
    private final TestInfo testInfo;

    /** Constructor. */
    public IgniteRpcTest(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    @AfterEach
    public void shutdownExecutor() {
        ExecutorServiceHelper.shutdownAndAwaitTermination(requestExecutor);
    }

    @Override
    public RpcServer<?> createServer() {
        ClusterService service = ClusterServiceTestUtils.clusterService(
                testInfo,
                INIT_PORT,
                new StaticNodeFinder(Collections.emptyList())
        );

        NodeOptions nodeOptions = new NodeOptions();

        requestExecutor = JRaftUtils.createRequestExecutor(nodeOptions);

        var server = new TestIgniteRpcServer(service, new NodeManager(), nodeOptions, requestExecutor) {
            @Override public void shutdown() {
                super.shutdown();

                assertThat(service.stopAsync(ForkJoinPool.commonPool()), willCompleteSuccessfully());
            }
        };

        assertThat(service.startAsync(ForkJoinPool.commonPool()), willCompleteSuccessfully());

        return server;
    }

    @Override
    public RpcClient createClient0() {
        int i = cntr.incrementAndGet();

        ClusterService service = ClusterServiceTestUtils.clusterService(
                testInfo,
                INIT_PORT - i,
                new StaticNodeFinder(List.of(new NetworkAddress(TestUtils.getLocalAddress(), INIT_PORT)))
        );

        IgniteRpcClient client = new IgniteRpcClient(service) {
            @Override public void shutdown() {
                super.shutdown();

                assertThat(service.stopAsync(ForkJoinPool.commonPool()), willCompleteSuccessfully());
            }
        };

        assertThat(service.startAsync(ForkJoinPool.commonPool()), willCompleteSuccessfully());

        waitForTopology(client, 1 + i, 5_000);

        return client;
    }

    @Override
    protected boolean waitForTopology(RpcClient client, int expected, long timeout) {
        ClusterService service = ((IgniteRpcClient) client).clusterService();

        boolean success = TestUtils.waitForTopology(service, expected, timeout);

        if (!success) {
            Collection<ClusterNode> topology = service.topologyService().allMembers();

            LOG.error("Topology on node '{}' didn't match expected topology size. Expected: {}, actual: {}.\nTopology nodes: {}",
                    service.nodeName(),
                    expected,
                    topology.size(),
                    topology.stream().map(ClusterNode::name).collect(toList())
            );
        }

        return success;
    }
}
