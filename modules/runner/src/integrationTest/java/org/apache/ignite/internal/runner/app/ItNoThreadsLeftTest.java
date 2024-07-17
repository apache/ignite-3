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

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * The test checks that no threads left after one node stopped.
 */
public class ItNoThreadsLeftTest extends IgniteAbstractTest {
    /** Short table name. */
    private static final String TABLE_NAME = "TBL1";

    private static final List<String> THREAD_NAMES_BLACKLIST = List.of(
            "nioEventLoopGroup",
            "globalEventExecutor",
            "ForkJoinPool",
            "process reaper",
            "CompletableFutureDelayScheduler",
            "parallel",
            "FastTimestamps updater"
    );

    /** One node cluster configuration. */
    private static final String NODE_CONFIGURATION =
            "{\n"
            + "  network.port: 3344,\n"
            + "  network.nodeFinder.netClusterNodes: [ \"localhost:3344\" ]\n"
            + "}";

    /**
     * Starts one node and stops it and checks that the amount of thread equivalent as before.
     *
     * @param testInfo JUnit meta info for the test.
     * @throws Exception If failed.
     */
    @Test
    public void test(TestInfo testInfo) throws Exception {
        Set<Thread> threadsBefore = getCurrentThreads();

        IgniteServer node = startNode(testInfo);
        try {
            Table tbl = createTable(node.api(), TABLE_NAME);

            assertNotNull(tbl);
        } finally {
            node.shutdown();
        }

        boolean threadsKilled = waitForCondition(() -> getCurrentThreads().size() <= threadsBefore.size(), 10, 3_000);

        if (!threadsKilled) {
            String leakedThreadNames = getCurrentThreads().stream()
                    .filter(thread -> !threadsBefore.contains(thread))
                    .map(Thread::getName)
                    .collect(joining(", "));

            fail(leakedThreadNames);
        }
    }

    private IgniteServer startNode(TestInfo testInfo) {
        String nodeName = IgniteTestUtils.testNodeName(testInfo, 0);

        IgniteServer node = TestIgnitionManager.start(nodeName, NODE_CONFIGURATION, workDir.resolve(nodeName));

        InitParameters initParameters = InitParameters.builder()
                .metaStorageNodes(node)
                .clusterName("cluster")
                .build();
        node.initCluster(initParameters);

        assertThat(node.waitForInitAsync(), willCompleteSuccessfully());

        return node;
    }

    /**
     * Creates a table.
     *
     * @param node Cluster node.
     * @param tableName Table name.
     */
    private static Table createTable(Ignite node, String tableName) {
        node.sql().execute(null, "CREATE TABLE " + tableName + "(key BIGINT PRIMARY KEY, valint INT,"
                + " valstr VARCHAR NOT NULL DEFAULT 'default')");

        return node.tables().table(tableName);
    }

    /**
     * Get a set threads.
     * TODO: IGNITE-15161. Filter will be removed after the stopping for all components is implemented.
     *
     * @return Set of threads.
     */
    private static Set<Thread> getCurrentThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> THREAD_NAMES_BLACKLIST.stream().noneMatch(thread.getName()::startsWith))
                .collect(Collectors.toSet());
    }
}
