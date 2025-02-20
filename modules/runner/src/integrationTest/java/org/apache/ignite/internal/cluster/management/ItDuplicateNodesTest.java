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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.internal.app.IgniteServerImpl;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
class ItDuplicateNodesTest extends BaseIgniteAbstractTest {
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

    @AfterEach
    void shutdownNodes() {
        servers.values().forEach(IgniteServer::shutdown);
        servers.clear();
    }

    @Test
    void physicalTopology(TestInfo testInfo) {
        int nodesCount = 2;

        IgniteServer node1 = startEmbeddedNode(testInfo, 0, nodesCount);
        IgniteServer node2 = startEmbeddedNode(testInfo, 1, nodesCount);

        assertThat(node1.name(), is(equalTo(node2.name())));

        await().untilAsserted(() -> {
            assertThat(((IgniteServerImpl) node1).igniteImpl().clusterService().topologyService().allMembers(), hasSize(nodesCount));
            assertThat(((IgniteServerImpl) node2).igniteImpl().clusterService().topologyService().allMembers(), hasSize(nodesCount));
        });
    }

    private IgniteServer startEmbeddedNode(TestInfo testInfo, int nodeIndex, int nodesCount) {
        String config = IgniteStringFormatter.format(
                NODE_BOOTSTRAP_CFG_TEMPLATE,
                DEFAULT_BASE_PORT + nodeIndex,
                seedAddressesString(nodesCount),
                DEFAULT_BASE_CLIENT_PORT + nodeIndex,
                DEFAULT_BASE_HTTP_PORT + nodeIndex
        );

        IgniteServer server = TestIgnitionManager.start(
                testNodeNameWithoutIndex(testInfo),
                config,
                WORK_DIR.resolve(testNodeName(testInfo, nodeIndex))
        );
        servers.put(nodeIndex, server);
        return server;
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
}
