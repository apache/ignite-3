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

package org.apache.ignite.raft;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.MessageSerializationRegistryImpl;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.raft.jraft.RaftMessageGroup;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.raft.jraft.test.TestUtils.getLocalAddress;

public class ClusterServiceTest {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(ClusterServiceTest.class);

    /** Network factory. */
    protected static final ClusterServiceFactory NETWORK_FACTORY = new TestScaleCubeClusterServiceFactory();

    /**
     *
     */
    private static final MessageSerializationRegistry SERIALIZATION_REGISTRY = new MessageSerializationRegistryImpl();

    /**
     * Server port offset.
     */
    protected static final int PORT_1 = 2001;
    protected static final int PORT_2 = 5006;

    /**
     * Raft message factory.
     * <p>
     * Has a default value for easier testing. Should always be set externally in the production code.
     */
    private RaftMessagesFactory raftMessagesFactory = new RaftMessagesFactory();

    @Test
    @Disabled
    public void test() throws Exception {
        ClusterService service1 = clusterService("server1", PORT_1, List.of(new NetworkAddress(getLocalAddress(), PORT_2)), true);
        ClusterService service2 = clusterService("server2", PORT_2, List.of(new NetworkAddress(getLocalAddress(), PORT_1)), true);

        RpcRequests.AppendEntriesRequest req = raftMessagesFactory.appendEntriesRequest().build();
        RpcRequests.AppendEntriesResponse resp = raftMessagesFactory.appendEntriesResponse().build();

        service1.messagingService().addMessageHandler(RaftMessageGroup.class, (message, address, correlationId) -> {
            LOG.info("Message received into server1 [correlationId={}]", correlationId);
        });

        service2.messagingService().addMessageHandler(RaftMessageGroup.class, (message, address, correlationId) -> {
            LOG.info("Message received into server2 [correlationId={}]", correlationId);

            service2.messagingService().send(new NetworkAddress(getLocalAddress(), PORT_1), req, correlationId);
        });

        Thread[] cpuLoadThreads = new Thread[16];

        for (int i = 0; i < 16; i++) {
            cpuLoadThreads[i] = new Thread(() -> {
                System.out.println("Thread " + Thread.currentThread().getName() + " started");
                double val = 10;
                for (; ; ) {
                    Math.atan(Math.sqrt(Math.pow(val, 10)));
                }
            });

            cpuLoadThreads[i].start();
        }

        while (true) {
            long start = System.currentTimeMillis();

            var fut = service1.messagingService().invoke(new NetworkAddress(getLocalAddress(), PORT_2), resp, 10_000);

            fut.get(10, TimeUnit.SECONDS);

            LOG.info("Message processed {}ms", System.currentTimeMillis() - start);

            Thread.sleep(500);
        }

    }

    /**
     * @param name Node name.
     * @param port Local port.
     * @param servers Server nodes of the cluster.
     * @return The client cluster view.
     */
    protected ClusterService clusterService(String name, int port, List<NetworkAddress> servers, boolean start) {
        var context = new ClusterLocalConfiguration(name, port, new StaticNodeFinder(servers), SERIALIZATION_REGISTRY);

        var network = NETWORK_FACTORY.createClusterService(context);

        if (start)
            network.start();

        return network;
    }
}
