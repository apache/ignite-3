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

package org.apache.ignite.internal.network.file;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.utils.ClusterServiceTestUtils.clusterService;
import static org.apache.ignite.utils.ClusterServiceTestUtils.findLocalAddresses;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.configuration.FileTransferringConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeFinder;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.network.TopologyEventHandler;
import org.junit.jupiter.api.TestInfo;

/**
 * Test cluster.
 */
public class TestCluster {
    /** Members of the cluster. */
    final List<Node> members;

    /** Latch that is locked until all members are visible in the topology. */
    private final CountDownLatch startupLatch;

    /** Node finder. */
    private final NodeFinder nodeFinder;

    private final FileTransferringConfiguration configuration;

    /**
     * Creates a test cluster with the given amount of members.
     *
     * @param numOfNodes Amount of cluster members.
     * @param testInfo Test info.
     */
    TestCluster(int numOfNodes, FileTransferringConfiguration configuration, Path workDir, TestInfo testInfo) {
        this.startupLatch = new CountDownLatch(numOfNodes - 1);

        int initialPort = 3344;

        List<NetworkAddress> addresses = findLocalAddresses(initialPort, initialPort + numOfNodes);

        this.nodeFinder = new StaticNodeFinder(addresses);
        this.configuration = configuration;

        var isInitial = new AtomicBoolean(true);

        this.members = addresses.stream()
                .map(addr -> startNode(workDir, testInfo, addr, isInitial.getAndSet(false)))
                .collect(toUnmodifiableList());
    }

    /**
     * Start cluster node.
     *
     * @param testInfo Test info.
     * @param addr Node address.
     * @param initial Whether this node is the first one.
     * @return Started cluster node.
     */
    private Node startNode(
            Path workDir, TestInfo testInfo, NetworkAddress addr, boolean initial
    ) {
        ClusterService clusterSvc = clusterService(testInfo, addr.port(), nodeFinder);

        if (initial) {
            clusterSvc.topologyService().addEventHandler(new TopologyEventHandler() {
                /** {@inheritDoc} */
                @Override
                public void onAppeared(ClusterNode member) {
                    startupLatch.countDown();
                }
            });
        }

        try {
            Path nodeDir = Files.createDirectory(workDir.resolve("node-" + clusterSvc.nodeName()));
            FileTransferringServiceImpl fileTransferringService = new FileTransferringServiceImpl(
                    clusterSvc.nodeName(),
                    clusterSvc.messagingService(),
                    configuration,
                    nodeDir
            );
            return new Node(nodeDir, clusterSvc, fileTransferringService);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Starts and waits for the cluster to come up.
     *
     * @throws InterruptedException If failed.
     * @throws AssertionError If the cluster was unable to start in 3 seconds.
     */
    void startAwait() throws InterruptedException {
        members.forEach(Node::start);

        if (!startupLatch.await(3, TimeUnit.SECONDS)) {
            throw new AssertionError();
        }
    }

    /**
     * Stops the cluster.
     */
    void shutdown() throws Exception {
        for (Node member : members) {
            member.stop();
        }
    }

    /**
     * Cluster node.
     */
    public static class Node {
        private final Path workDir;
        private final ClusterService clusterService;
        private final FileTransferringService fileTransferringService;
        private final List<IgniteComponent> components;

        /**
         * Constructor.
         *
         * @param workDir Work directory.
         * @param clusterService Cluster service.
         * @param fileTransferringService File transferring service.
         */
        public Node(Path workDir, ClusterService clusterService, FileTransferringService fileTransferringService) {
            this.workDir = workDir;
            this.clusterService = clusterService;
            this.fileTransferringService = fileTransferringService;
            this.components = List.of(clusterService, fileTransferringService);
        }

        public String nodeName() {
            return clusterService.nodeName();
        }

        public Path workDir() {
            return workDir;
        }

        public ClusterService clusterService() {
            return clusterService;
        }

        public FileTransferringService fileTransferringService() {
            return fileTransferringService;
        }

        void start() {
            components.forEach(IgniteComponent::start);
        }

        void stop() throws Exception {
            IgniteUtils.closeAll(Stream.concat(
                    components.stream().map(c -> c::beforeNodeStop),
                    components.stream().map(c -> c::stop)
            ));
        }
    }
}
