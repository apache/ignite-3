/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
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

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter.convert;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * The test checks that no threads left after one node stopped.
 */
public class ItNoThreadsLeftTest extends IgniteAbstractTest {
    /** Schema name. */
    private static final String SCHEMA = "PUBLIC";

    /** Short table name. */
    private static final String SHORT_TABLE_NAME = "tbl1";

    private static final List<String> THREAD_NAMES_BLACKLIST = List.of(
            "nioEventLoopGroup",
            "globalEventExecutor",
            "ForkJoinPool",
            "process reaper",
            "CompletableFutureDelayScheduler",
            "parallel"
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

        try {
            Ignite ignite = startNode(testInfo);

            Table tbl = createTable(ignite, SCHEMA, SHORT_TABLE_NAME);

            assertNotNull(tbl);
        } finally {
            stopNode(testInfo);
        }

        boolean threadsKilled = waitForCondition(() -> threadsBefore.size() == getCurrentThreads().size(), 3_000);

        if (!threadsKilled) {
            String leakedThreadNames = getCurrentThreads().stream()
                    .filter(thread -> !threadsBefore.contains(thread))
                    .map(Thread::getName)
                    .collect(joining(", "));

            fail(leakedThreadNames);
        }
    }

    private Ignite startNode(TestInfo testInfo) {
        String nodeName = IgniteTestUtils.testNodeName(testInfo, 0);

        CompletableFuture<Ignite> future = IgnitionManager.start(nodeName, NODE_CONFIGURATION, workDir.resolve(nodeName));

        IgnitionManager.init(nodeName, List.of(nodeName));

        assertThat(future, willCompleteSuccessfully());

        return future.join();
    }

    private static void stopNode(TestInfo testInfo) {
        String nodeName = IgniteTestUtils.testNodeName(testInfo, 0);

        IgnitionManager.stop(nodeName);
    }

    /**
     * Creates a table.
     *
     * @param node Cluster node.
     * @param schemaName Schema name.
     * @param shortTableName Table name.
     */
    private static Table createTable(Ignite node, String schemaName, String shortTableName) {
        return node.tables().createTable(
                schemaName + "." + shortTableName, tblCh -> convert(SchemaBuilders.tableBuilder(schemaName, shortTableName).columns(
                                SchemaBuilders.column("key", ColumnType.INT64).build(),
                                SchemaBuilders.column("valInt", ColumnType.INT32).asNullable(true).build(),
                                SchemaBuilders.column("valStr", ColumnType.string()).withDefaultValueExpression("default").build()
                        ).withPrimaryKey("key").build(),
                        tblCh).changeReplicas(2).changePartitions(10)
        );
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
