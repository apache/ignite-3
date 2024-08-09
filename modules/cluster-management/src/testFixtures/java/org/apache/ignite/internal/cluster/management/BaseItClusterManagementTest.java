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

package org.apache.ignite.internal.cluster.management;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Base class for integration tests that use a cluster of {@link MockNode}s.
 */
@ExtendWith(ConfigurationExtension.class)
public abstract class BaseItClusterManagementTest extends IgniteAbstractTest {
    private static final int PORT_BASE = 10000;

    @InjectConfiguration
    private static RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private static NodeAttributesConfiguration userNodeAttributes;

    @InjectConfiguration
    private static StorageConfiguration storageConfiguration;

    private TestInfo testInfo;

    @BeforeEach
    void setTestInfo(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    protected List<MockNode> createNodes(int numNodes) {
        StaticNodeFinder nodeFinder = createNodeFinder(numNodes);

        return IntStream.range(0, numNodes)
                .mapToObj(i -> new MockNode(
                        testInfo,
                        nodeFinder.findNodes().get(i),
                        nodeFinder,
                        workDir,
                        raftConfiguration,
                        userNodeAttributes,
                        storageConfiguration

                ))
                .collect(toList());
    }

    protected MockNode addNodeToCluster(Collection<MockNode> cluster) {
        MockNode node = createNode(cluster.size(), cluster.size());

        cluster.add(node);

        return node;
    }

    protected MockNode createNode(int idx, int clusterSize) {
        return new MockNode(
                testInfo,
                new NetworkAddress("localhost", PORT_BASE + idx),
                createNodeFinder(clusterSize),
                workDir,
                raftConfiguration,
                userNodeAttributes,
                storageConfiguration
        );
    }

    protected static void stopNodes(Collection<MockNode> nodes) throws Exception {
        closeAll(nodes.parallelStream().map(node -> node::beforeNodeStop));
        closeAll(nodes.parallelStream().map(node -> node::stop));
    }

    private static StaticNodeFinder createNodeFinder(int clusterSize) {
        return IntStream.range(0, clusterSize)
                .mapToObj(i -> new NetworkAddress("localhost", PORT_BASE + i))
                .collect(collectingAndThen(toUnmodifiableList(), StaticNodeFinder::new));
    }
}
