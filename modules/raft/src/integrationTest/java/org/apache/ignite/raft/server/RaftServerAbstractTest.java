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

package org.apache.ignite.raft.server;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Abstract test for raft server.
 */
@ExtendWith(ConfigurationExtension.class)
abstract class RaftServerAbstractTest extends IgniteAbstractTest {
    protected static final RaftMessagesFactory FACTORY = new RaftMessagesFactory();

    /**
     * Server port offset.
     */
    protected static final int PORT = 20010;

    /** Raft configuration. */
    @InjectConfiguration
    protected RaftConfiguration raftConfiguration;

    /** Test info. */
    TestInfo testInfo;

    private final List<ClusterService> clusterServices = new ArrayList<>();

    @BeforeEach
    void initTestInfo(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    @AfterEach
    protected void after() throws Exception {
        clusterServices.forEach(ClusterService::stop);
    }

    /**
     * Creates a client cluster view.
     *
     * @param port    Local port.
     * @param servers Server nodes of the cluster.
     * @return The client cluster view.
     */
    protected ClusterService clusterService(int port, List<NetworkAddress> servers, boolean start) {
        var network = ClusterServiceTestUtils.clusterService(
                testInfo,
                port,
                new StaticNodeFinder(servers)
        );

        if (start) {
            network.start();
        }

        clusterServices.add(network);

        return network;
    }

    protected JraftServerImpl jraftServer(List<JraftServerImpl> servers, int idx, ClusterService service, NodeOptions opts) {
        Path dataPath = workDir.resolve("node" + idx);

        return new JraftServerImpl(
                service,
                dataPath,
                raftConfiguration,
                opts,
                new RaftGroupEventsClientListener()
        ) {
            @Override
            public void stop() throws Exception {
                servers.remove(this);

                super.stop();

                service.stop();
            }
        };
    }
}
