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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Integration tests running against a single Ignite node.
 */
public class ItSingleNodeTest extends IgniteAbstractTest {
    private static final String NODE_CONFIG = "ignite.network.nodeFinder.netClusterNodes: [ \"localhost:" + DEFAULT_PORT + "\"]";

    private static final String TABLE_NAME = "TEST_TABLE";

    private String nodeName;

    private IgniteServer node;

    private Ignite ignite;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        nodeName = testNodeName(testInfo, 0);

        node = TestIgnitionManager.start(nodeName, NODE_CONFIG, workDir.resolve(nodeName));

        InitParameters parameters = new InitParametersBuilder()
                .clusterName("cluster")
                .metaStorageNodes(node)
                .build();

        node.initCluster(parameters);

        assertThat(node.waitForInitAsync(), willCompleteSuccessfully());

        ignite = node.api();
    }

    @AfterEach
    void tearDown() {
        node.shutdown();
    }

    /**
     * Kind of a stress test that inserts some tuples after a node has been restarted.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-20911")
    @Test
    void testManyPutsAfterRestart() {
        String sqlCreate = "CREATE TABLE " + TABLE_NAME + "(\n"
                + "    key int PRIMARY KEY,\n"
                + "    field1   int\n"
                + ")";

        ignite.sql().execute(null, sqlCreate);

        node.shutdown();

        node = TestIgnitionManager.start(nodeName, NODE_CONFIG, workDir.resolve(nodeName));
        assertThat(node.waitForInitAsync(), willCompleteSuccessfully());

        ignite = node.api();

        KeyValueView<Tuple, Tuple> keyValueView = ignite.tables().table(TABLE_NAME).keyValueView();

        Tuple testValue = Tuple.create().set("field1", 239);

        for (int i = 0; i < 2000; i++) {
            keyValueView.put(null, Tuple.create().set("key", i), testValue);
        }
    }
}
