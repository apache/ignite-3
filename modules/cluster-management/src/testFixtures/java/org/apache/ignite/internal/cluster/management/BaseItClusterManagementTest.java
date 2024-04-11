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

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.cluster.management.configuration.ClusterManagementConfiguration;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Base class for integration tests that use a cluster of {@link MockNode}s.
 */
@ExtendWith(ConfigurationExtension.class)
public abstract class BaseItClusterManagementTest extends BaseIgniteAbstractTest {
    private static final int PORT_BASE = 10000;

    @InjectConfiguration
    private static ClusterManagementConfiguration cmgConfiguration;

    @InjectConfiguration
    private static RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private static NodeAttributesConfiguration userNodeAttributes;

    @InjectConfiguration
    private static StorageConfiguration storageConfiguration;

    protected static List<MockNode> createNodes(int numNodes, TestInfo testInfo, Path workDir) {
        StaticNodeFinder nodeFinder = createNodeFinder(numNodes);

        return IntStream.range(0, numNodes)
                .mapToObj(i -> new MockNode(
                        testInfo,
                        nodeFinder.findNodes().get(i),
                        nodeFinder,
                        workDir.resolve("node" + i),
                        raftConfiguration,
                        cmgConfiguration,
                        userNodeAttributes,
                        storageConfiguration

                ))
                .collect(toList());
    }

    protected static MockNode addNodeToCluster(Collection<MockNode> cluster, TestInfo testInfo, Path workDir) {
        var node = new MockNode(
                testInfo,
                new NetworkAddress("localhost", PORT_BASE + cluster.size()),
                createNodeFinder(cluster.size()),
                workDir.resolve("node" + cluster.size()),
                raftConfiguration,
                cmgConfiguration,
                userNodeAttributes,
                storageConfiguration
        );

        cluster.add(node);

        return node;
    }

    protected static void stopNodes(Collection<MockNode> nodes) throws Exception {
        IgniteUtils.closeAll(Stream.concat(
                nodes.stream().map(node -> node::beforeNodeStop),
                nodes.stream().map(node -> node::stop)
        ));
    }

    private static StaticNodeFinder createNodeFinder(int clusterSize) {
        return IntStream.range(0, clusterSize)
                .mapToObj(i -> new NetworkAddress("localhost", PORT_BASE + i))
                .collect(collectingAndThen(toUnmodifiableList(), StaticNodeFinder::new));
    }
}
