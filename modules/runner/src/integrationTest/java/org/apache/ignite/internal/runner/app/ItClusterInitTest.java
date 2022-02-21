/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.runner.app;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.join.Leaders;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.NodeStoppingException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests for initializing a cluster.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItClusterInitTest {
    @WorkDirectory
    private Path workDir;

    private final List<IgniteImpl> nodes = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(nodes);
    }

    /**
     * Tests the happy case for cluster initialization.
     */
    @Test
    void testNormalInit(TestInfo testInfo) throws NodeStoppingException {
        createCluster(testInfo, 4);

        List<String> metastorageNodes = List.of(nodes.get(0).name(), nodes.get(1).name());

        List<String> cmgNodes = List.of(nodes.get(2).name(), nodes.get(3).name());

        Leaders leaders = nodes.get(0).init(metastorageNodes, cmgNodes);

        assertThat(metastorageNodes, hasItem(leaders.metaStorageLeader()));
        assertThat(metastorageNodes, not(hasItem(leaders.cmgLeader())));

        assertThat(cmgNodes, hasItem(leaders.cmgLeader()));
        assertThat(cmgNodes, not(hasItem(leaders.metaStorageLeader())));
    }

    /**
     * Tests a scenario when a cluster is initialized twice.
     */
    @Test
    void testDoubleInit(TestInfo testInfo) throws NodeStoppingException {
        createCluster(testInfo, 1);

        IgniteImpl node = nodes.get(0);

        node.init(List.of(node.name()));

        assertThrows(IgniteInternalException.class, () -> node.init(List.of(node.name())));
    }

    /**
     * Tests a scenario when some nodes are stopped during initialization.
     */
    @Test
    void testInitStoppingNodes(TestInfo testInfo) {
        createCluster(testInfo, 2);

        IgniteImpl node1 = nodes.get(0);
        IgniteImpl node2 = nodes.get(1);

        node2.stop();

        assertThrows(IgniteInternalException.class, () -> node1.init(List.of(node1.name(), node2.name())));

        node1.stop();

        assertThrows(NodeStoppingException.class, () -> node1.init(List.of(node1.name(), node2.name())));
    }

    private void createCluster(TestInfo testInfo, int numNodes) {
        int[] ports = IntStream.range(3344, 3344 + numNodes).toArray();

        String nodeFinderConfig = Arrays.stream(ports)
                .mapToObj(port -> String.format("\"localhost:%d\"", port))
                .collect(Collectors.joining(", ", "[", "]"));

        Arrays.stream(ports)
                .mapToObj(port -> {
                    String config = "{"
                            + " network.port: " + port + ","
                            + " network.nodeFinder.netClusterNodes: " + nodeFinderConfig
                            + "}";

                    String nodeName = testNodeName(testInfo, port);

                    return (IgniteImpl) IgnitionManager.start(nodeName, config, workDir.resolve(nodeName));
                })
                .forEach(nodes::add);
    }
}
