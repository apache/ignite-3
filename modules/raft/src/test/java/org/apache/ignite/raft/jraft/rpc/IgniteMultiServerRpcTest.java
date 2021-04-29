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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcServer;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.raft.jraft.test.TestUtils.INIT_PORT;
import static org.apache.ignite.raft.jraft.test.TestUtils.getMyIp;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Contains additional tests for topology. */
@Ignore // TODO asch remove.
public class IgniteMultiServerRpcTest extends IgniteRpcTest {
    /** */
    private static final IgniteLogger LOG = IgniteLogger.forClass(IgniteMultiServerRpcTest.class);

    /** */
    private AtomicInteger cntr = new AtomicInteger();

    /** */
    private List<Endpoint> endpoints = IntStream.range(0, 5).
        mapToObj(val -> new Endpoint(TestUtils.getMyIp(), INIT_PORT + val)).collect(Collectors.toList());

    /** {@inheritDoc} */
    @Override public void setup() {
        endpoint = endpoints.get(0);

        for (Endpoint endpoint : endpoints) {
            IgniteRpcServer server = (IgniteRpcServer) createServer(endpoint);
            server.registerProcessor(new Request1RpcProcessor());
            server.registerProcessor(new Request2RpcProcessor());
            server.init(null);
            servers.add(server);
        }
    }

    /** {@inheritDoc} */
    @Override public RpcServer createServer(Endpoint endpoint) {
        ClusterService service = createService(endpoint.toString(), endpoint.getPort(),
            endpoints.stream().map(e -> e.toString()).collect(Collectors.toList()));

        return new IgniteRpcServer(service, false, new NodeManager());
    }

    /** */
    @Test
    public void testFailureDetection() {
        for (RpcServer rpcServer : servers) {
            IgniteRpcServer rpcServer0 = (IgniteRpcServer) rpcServer;

            assertTrue(waitForTopology(rpcServer0.clusterService(), 5, 5_000));
        }

        int idx0 = 0;
        int idx1 = 2;

        servers.get(idx0).shutdown();
        servers.get(idx1).shutdown();

        RpcServer svc0 = createServer(new Endpoint(getMyIp(), INIT_PORT + idx0));
        svc0.init(null);
        servers.set(idx0, svc0);

        RpcServer svc2 = createServer(new Endpoint(getMyIp(), INIT_PORT + idx1));
        svc2.init(null);
        servers.set(idx1, svc2);

        for (RpcServer rpcServer : servers) {
            IgniteRpcServer rpcServer0 = (IgniteRpcServer) rpcServer;

            assertTrue(waitForTopology(rpcServer0.clusterService(), 5, 5_000),
                rpcServer0.clusterService().topologyService().localMember().toString());
        }
    }
}
