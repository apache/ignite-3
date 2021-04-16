/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.raft.jraft.rpc;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.message.MessageSerializationRegistry;
import org.apache.ignite.network.scalecube.ScaleCubeClusterServiceFactory;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcClient;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcServer;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.junit.jupiter.api.Disabled;

/** */
public class IgniteRpcTest extends AbstractRpcTest {
    private static final IgniteLogger LOG = IgniteLogger.forClass(IgniteRpcTest.class);

    private AtomicInteger cntr = new AtomicInteger();

    @Override public RpcServer createServer(Endpoint endpoint) {
        ClusterService service = createService(endpoint.toString(), endpoint.getPort(), List.of());

        return new IgniteRpcServer(service, false);
    }

    @Override public RpcClient createClient() {
        int i = cntr.incrementAndGet();

        ClusterService service = createService("client" + i, endpoint.getPort() + i, List.of(endpoint.toString()));

        return new IgniteRpcClient(service, false);
    }

    /**
     * @param name Node name.
     * @param port Local port.
     * @param servers Server nodes of the cluster.
     * @return The client cluster view.
     */
    private ClusterService createService(String name, int port, List<String> servers) {
        // TODO: IGNITE-14088: Uncomment and use real serializer provider
        var serializationRegistry = new MessageSerializationRegistry();

        var context = new ClusterLocalConfiguration(name, port, servers, serializationRegistry);
        var factory = new ScaleCubeClusterServiceFactory();

        return factory.createClusterService(context);
    }

    /**
     * @param service The service.
     * @param expected Expected count.
     * @param timeout The timeout in millis.
     * @return {@code True} if topology size is equal to expected.
     */
    private boolean waitForNode(ClusterService service, String id, int timeout) {
        long stop = System.currentTimeMillis() + timeout;

        while(System.currentTimeMillis() < stop) {
            if (service.topologyService().allMembers().stream().map(x -> x.name()).anyMatch(x -> x.equals(id)))
                return true;

            try {
                Thread.sleep(50);
            }
            catch (InterruptedException e) {
                return false;
            }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override protected boolean waitForTopology(RpcClient client, int expected, long timeout) {
        IgniteRpcClient client0 = (IgniteRpcClient) client;

        ClusterService service = client0.clusterService();

        long stop = System.currentTimeMillis() + timeout;

        while(System.currentTimeMillis() < stop) {
            if (service.topologyService().allMembers().size() == expected)
                return true;

            try {
                Thread.sleep(50);
            }
            catch (InterruptedException e) {
                return false;
            }
        }

        return false;
    }
}
