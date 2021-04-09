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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.lang.LogWrapper;
import org.apache.ignite.network.Network;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.scalecube.ScaleCubeMemberResolver;
import org.apache.ignite.network.scalecube.ScaleCubeNetworkClusterFactory;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcClient;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcServer;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.junit.After;
import org.junit.Test;
import org.junit.jupiter.api.Disabled;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** */
@Disabled
public class IgniteRpcTest extends AbstractRpcTest {
    private static final LogWrapper LOG = new LogWrapper(IgniteRpcTest.class);

    public static final int PORT = 20000;

    private NetworkCluster serverNode;

    private List<NetworkCluster> clients = new CopyOnWriteArrayList<>();

    private AtomicInteger cntr = new AtomicInteger();

    @After
    @Override public void teardown() {
        super.teardown();

        try {
            serverNode.shutdown();
        }
        catch (Exception e) {
            fail(e.toString());
        }

        for (NetworkCluster node : clients) {
            try {
                node.shutdown();
            }
            catch (Exception e) {
                fail(e.toString());
            }
        }
    }

    @Override public RpcServer createServer(Endpoint endpoint) {
        serverNode = startNode(endpoint.toString(), endpoint.getPort(), List.of());

        return new IgniteRpcServer(serverNode);
    }

    @Override public RpcClient createClient() {
        int i = cntr.incrementAndGet();

        NetworkCluster clientNode = startNode("client" + i, PORT + i, List.of(serverNode.localMember().name()));

        assertTrue("The server is not found in the topology", waitForNode(clientNode, serverNode.localMember().name(), 5_000));

        clients.add(clientNode);

        return new IgniteRpcClient(clientNode);
    }

    /**
     * @param name Node name.
     * @param port Local port.
     * @param servers Server nodes of the cluster.
     * @return The client cluster view.
     */
    private NetworkCluster startNode(String name, int port, List<String> servers) {
        Network network = new Network(
            new ScaleCubeNetworkClusterFactory(name, port, servers, new ScaleCubeMemberResolver())
        );

        // TODO: IGNITE-14088: Uncomment and use real serializer provider
//        network.registerMessageMapper((short)1000, new DefaultMessageMapperProvider());
//        network.registerMessageMapper((short)1001, new DefaultMessageMapperProvider());
//        network.registerMessageMapper((short)1005, new DefaultMessageMapperProvider());
//        network.registerMessageMapper((short)1006, new DefaultMessageMapperProvider());
//        network.registerMessageMapper((short)1009, new DefaultMessageMapperProvider());

        LOG.info("Starting node id={0} port={1} peers={2}", name, port, servers);

        return network.start();
    }

    /**
     * @param cluster The cluster.
     * @param expected Expected count.
     * @param timeout The timeout in millis.
     * @return {@code True} if topology size is equal to expected.
     */
    private boolean waitForNode(NetworkCluster cluster, String id, int timeout) {
        long stop = System.currentTimeMillis() + timeout;

        while(System.currentTimeMillis() < stop) {
            if (cluster.allMembers().stream().map(x -> x.name()).anyMatch(x -> x.equals(id)))
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

    @Override @Test
    public void testDisconnect() {
        RpcClient client1 = createClient();
        RpcClient client2 = createClient();

        Endpoint srv = new Endpoint("localhost", 1000);
        assertTrue(client1.checkConnection(srv));
        assertTrue(client2.checkConnection(srv));

        try {
            serverNode.shutdown();

            Thread.sleep(1000);
        }
        catch (Exception e) {
            fail(e.getMessage());
        }

        assertFalse(client1.checkConnection(srv));
        assertFalse(client2.checkConnection(srv));
    }
}
