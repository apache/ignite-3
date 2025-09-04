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

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.ClusterConfiguration.DEFAULT_BASE_CLIENT_PORT;
import static org.apache.ignite.internal.ClusterConfiguration.DEFAULT_BASE_HTTP_PORT;
import static org.apache.ignite.internal.ClusterConfiguration.DEFAULT_BASE_PORT;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.shortTestMethodName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowWithCauseOrSuppressed;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.app.IgniteServerImpl;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.table.KeyValueView;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
class ItDuplicateNodeNamesTest extends BaseIgniteAbstractTest {
    private static final String NODE_BOOTSTRAP_CFG_TEMPLATE = "ignite {\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder.netClusterNodes: [ {} ]\n"
            + "  },\n"
            + "  clientConnector.port: {},\n"
            + "  rest.port: {},\n"
            + "}";

    @WorkDirectory
    private static Path WORK_DIR;

    private final Map<Integer, IgniteServer> servers = new HashMap<>();

    private TestInfo testInfo;

    @BeforeEach
    void saveTestInfo(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    @AfterEach
    void shutdownNodes() {
        servers.values().forEach(IgniteServer::shutdown);
        servers.clear();
    }

    @Test
    void duplicatesAreAllowedInPhysicalTopology() {
        int nodesCount = 2;

        // When started two nodes with the same node names
        String nodeName = testNodeNameWithoutIndex(testInfo);
        IgniteServer node1 = startEmbeddedNode(nodeName, 0, nodesCount);
        IgniteServer node2 = startEmbeddedNode(nodeName, 1, nodesCount);

        assertThat(node1.name(), is(equalTo(node2.name())));

        // Then both nodes are in the physical topology on each node
        await().untilAsserted(() -> {
            assertThat(getPhysicalTopologyMembers(node1), hasSize(nodesCount));
            assertThat(getPhysicalTopologyMembers(node2), hasSize(nodesCount));
        });
    }

    @Test
    void duplicatesAreNotAllowedInLogicalTopology() {
        int nodesCount = 3;

        // When first node is started with unique node name
        String firstNodeName = testNodeNameWithoutIndex(testInfo) + "_0";
        IgniteServer metaStorageAndCmgNode = startEmbeddedNode(firstNodeName, 0, nodesCount);

        // And two more nodes are started with the same node name
        String duplicateNodeName = testNodeNameWithoutIndex(testInfo);
        IgniteServer secondNode = startEmbeddedNode(duplicateNodeName, 1, nodesCount);
        startEmbeddedNode(duplicateNodeName, 2, nodesCount);

        InitParameters initParameters = InitParameters.builder()
                .metaStorageNodes(metaStorageAndCmgNode)
                .clusterName("cluster")
                .build();

        // Then cluster initialization fails because there are duplicate node names
        assertThat(
                metaStorageAndCmgNode.initClusterAsync(initParameters),
                willThrow(InitException.class, "Unable to initialize the cluster: Duplicate node name \"" + duplicateNodeName + "\"")
        );

        // When duplicate node is stopped
        stopNode(2);

        // And is removed from the physical topology on the first node
        await().until(() -> getPhysicalTopologyMembers(metaStorageAndCmgNode), hasSize(2));

        // Then cluster is initialized successfully
        assertThat(metaStorageAndCmgNode.initClusterAsync(initParameters), willCompleteSuccessfully());

        // When new node with the duplicate name is started
        IgniteServer newNode = startEmbeddedNode(duplicateNodeName, 2, nodesCount);

        // Then join request fails
        assertThat(
                newNode.waitForInitAsync(),
                willThrowWithCauseOrSuppressed(InitException.class, "Duplicate node name \"" + duplicateNodeName + "\"")
        );

        // And the cluster is operational

        metaStorageAndCmgNode.api().sql().execute(null, "CREATE TABLE TEST (id INT PRIMARY KEY, val VARCHAR)");
        metaStorageAndCmgNode.api().sql().execute(null, "INSERT INTO TEST VALUES (1, 'foo')");
        KeyValueView<Integer, String> kvView = secondNode.api().tables().table("TEST").keyValueView(Integer.class, String.class);
        assertThat(kvView.get(null, 1), is("foo"));
    }

    private IgniteServer startEmbeddedNode(String nodeName, int nodeIndex, int nodesCount) {
        String config = IgniteStringFormatter.format(
                NODE_BOOTSTRAP_CFG_TEMPLATE,
                DEFAULT_BASE_PORT + nodeIndex,
                seedAddressesString(nodesCount),
                DEFAULT_BASE_CLIENT_PORT + nodeIndex,
                DEFAULT_BASE_HTTP_PORT + nodeIndex
        );

        IgniteServer server = TestIgnitionManager.start(nodeName, config, WORK_DIR.resolve(getWorkDirName(nodeName, nodeIndex)));
        server.waitForInitAsync(); // Do nothing
        servers.put(nodeIndex, server);
        return server;
    }

    private static String getWorkDirName(String nodeName, int nodeIndex) {
        // Make sure the working directory name is unique and corresponds with the node index
        if (!nodeName.endsWith("_" + nodeIndex)) {
            return nodeName + "_" + nodeIndex;
        }
        return nodeName;
    }

    private void stopNode(int nodeIndex) {
        servers.computeIfPresent(nodeIndex, (index, server) -> {
            server.shutdown();
            return null;
        });
    }

    private static String seedAddressesString(int nodesCount) {
        return IntStream.range(0, nodesCount)
                .mapToObj(index -> "\"localhost:" + (DEFAULT_BASE_PORT + index) + '\"')
                .collect(joining(", "));
    }

    private static String testNodeNameWithoutIndex(TestInfo testInfo) {
        String testMethodName = testInfo.getTestMethod().map(Method::getName).orElse("null");
        String testClassName = testInfo.getTestClass().map(Class::getSimpleName).orElse("null");

        return IgniteStringFormatter.format("{}_{}",
                shortTestMethodName(testClassName),
                shortTestMethodName(testMethodName)
        );
    }

    private static Collection<InternalClusterNode> getPhysicalTopologyMembers(IgniteServer node) {
        return ((IgniteServerImpl) node).igniteImpl().clusterService().topologyService().allMembers();
    }
}
