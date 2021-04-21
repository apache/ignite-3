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

package org.apache.ignite.raft.server;

import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.message.MessageSerializationRegistry;
import org.apache.ignite.network.scalecube.ScaleCubeClusterServiceFactory;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.message.RaftClientMessageFactory;
import org.apache.ignite.raft.client.message.impl.RaftClientMessageFactoryImpl;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.client.service.impl.RaftGroupServiceImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** */
abstract class RaftCounterServerAbstractTest {
    /** */
    protected static final IgniteLogger LOG = IgniteLogger.forClass(RaftCounterServerAbstractTest.class);

    /** */
    protected static final RaftClientMessageFactory FACTORY = new RaftClientMessageFactoryImpl();

    /** Network factory. */
    protected static final ClusterServiceFactory NETWORK_FACTORY = new ScaleCubeClusterServiceFactory();

    /** */
    protected static final int PORT = 20010;

    /** TODO: IGNITE-14088: Uncomment and use real serializer provider */
    private static final MessageSerializationRegistry SERIALIZATION_REGISTRY = new MessageSerializationRegistry();

    /** */
    protected static final String COUNTER_GROUP_ID_0 = "counter0";

    /** */
    protected static final String COUNTER_GROUP_ID_1 = "counter1";

    /** */
    protected RaftGroupService client1;

    /** */
    protected RaftGroupService client2;

    /**
     */
    @Test
    public void testRefreshLeader() throws Exception {
        Peer leader = client1.leader();

        assertNull(leader);

        client1.refreshLeader().get();

        assertNotNull(client1.leader());
    }

    /**
     * @throws Exception
     */
    @Test
    public void testCounterCommandListener() throws Exception {
        client1.refreshLeader().get();
        client2.refreshLeader().get();

        assertNotNull(client1.leader());
        assertNotNull(client2.leader());

        assertEquals(2, client1.<Integer>run(new IncrementAndGetCommand(2)).get());
        assertEquals(2, client1.<Integer>run(new GetValueCommand()).get());
        assertEquals(3, client1.<Integer>run(new IncrementAndGetCommand(1)).get());
        assertEquals(3, client1.<Integer>run(new GetValueCommand()).get());

        assertEquals(4, client2.<Integer>run(new IncrementAndGetCommand(4)).get());
        assertEquals(4, client2.<Integer>run(new GetValueCommand()).get());
        assertEquals(7, client2.<Integer>run(new IncrementAndGetCommand(3)).get());
        assertEquals(7, client2.<Integer>run(new GetValueCommand()).get());
    }

    /**
     * @param name Node name.
     * @param port Local port.
     * @param servers Server nodes of the cluster.
     * @return The client cluster view.
     */
    protected ClusterService clusterService(String name, int port, List<String> servers, boolean start) {
        var context = new ClusterLocalConfiguration(name, port, servers, SERIALIZATION_REGISTRY);

        var network = NETWORK_FACTORY.createClusterService(context);

        if (start)
            network.start();

        return network;
    }

    /**
     * @param cluster The cluster.
     * @param expected Expected count.
     * @param timeout The timeout in millis.
     * @return {@code True} if topology size is equal to expected.
     */
    protected boolean waitForTopology(ClusterService cluster, int expected, int timeout) {
        long stop = System.currentTimeMillis() + timeout;

        while(System.currentTimeMillis() < stop) {
            if (cluster.topologyService().allMembers().size() >= expected)
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
