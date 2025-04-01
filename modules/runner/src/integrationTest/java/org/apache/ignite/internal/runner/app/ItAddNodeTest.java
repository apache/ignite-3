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

package org.apache.ignite.internal.runner.app;

import static org.apache.ignite.internal.network.configuration.NetworkConfigurationSchema.DEFAULT_PORT;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Integration tests for testing simple node join scenarios.
 */
public class ItAddNodeTest extends IgniteAbstractTest {
    private static final String NODE_CONFIG_TEMPLATE
            = "ignite.network.nodeFinder.netClusterNodes = [\"localhost:" + DEFAULT_PORT + "\"]\n"
            + "ignite.network.port = %d\n"
            + "ignite.rest.port = %d\n";

    /**
     * Test that shows that we can join a third node to a cluster of two.
     */
    @Test
    public void addOneNodeToTwoNodeCluster(TestInfo testInfo) throws Exception {
        int numberOfNodesInitially = 2;

        List<IgniteServer> nodes = new ArrayList<>();

        try {
            for (int i = 0; i < numberOfNodesInitially; i++) {
                String nodeName = testNodeName(testInfo, i);

                IgniteServer node = TestIgnitionManager.start(
                        nodeName,
                        String.format(NODE_CONFIG_TEMPLATE, DEFAULT_PORT + i, 10300 + i),
                        workDir.resolve(nodeName)
                );

                nodes.add(node);
            }

            IgniteServer node0 = nodes.get(0);

            node0.initCluster(new InitParametersBuilder()
                    .clusterName("clusterName")
                    .metaStorageNodes(node0)
                    .build()
            );
            assertThat(node0.waitForInitAsync(), willCompleteSuccessfully());

            Ignite ignite0 = node0.api();

            try (ResultSet<SqlRow> unused = ignite0.sql().execute(null, "CREATE TABLE " + "TEST_TABLE" + "(\n"
                    + "    key int PRIMARY KEY,\n"
                    + "    field1   int\n"
                    + ")"
            )) {
                // No-op.
            }

            String nodeName2 = testNodeName(testInfo, numberOfNodesInitially);
            IgniteServer node2 = TestIgnitionManager.start(
                    nodeName2,
                    String.format(NODE_CONFIG_TEMPLATE, DEFAULT_PORT + numberOfNodesInitially, 10300 + numberOfNodesInitially),
                    workDir.resolve(nodeName2)
            );

            nodes.add(node2);
        } finally {
            IgniteUtils.closeAll(nodes.stream());
        }
    }
}
