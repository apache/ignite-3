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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test for baseline changes.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItBaselineChangesTest {
    private static final int NUM_NODES = 3;

    /** Start network port for test nodes. */
    private static final int BASE_PORT = 3344;

    private final List<String> clusterNodeNames = new ArrayList<>();

    private final List<Ignite> clusterNodes = new ArrayList<>();

    @WorkDirectory
    private Path workDir;

    /**
     * Before each.
     */
    @BeforeEach
    void setUp(TestInfo testInfo) {
        List<CompletableFuture<Ignite>> futures = IntStream.range(0, NUM_NODES)
                .mapToObj(i -> startNodeAsync(testInfo, i))
                .collect(toList());

        String metaStorageNode = testNodeName(testInfo, BASE_PORT);

        IgnitionManager.init(metaStorageNode, List.of(metaStorageNode), "cluster");

        for (CompletableFuture<Ignite> future : futures) {
            assertThat(future, willCompleteSuccessfully());

            clusterNodes.add(future.join());
        }
    }

    /**
     * After each.
     */
    @AfterEach
    void tearDown() throws Exception {
        List<AutoCloseable> closeables = clusterNodeNames.stream()
                .map(name -> (AutoCloseable) () -> IgnitionManager.stop(name))
                .collect(toList());

        IgniteUtils.closeAll(closeables);
    }

    /**
     * Check dynamic table creation.
     */
    @Test
    void testBaselineExtending(TestInfo testInfo) {
        assertEquals(NUM_NODES, clusterNodes.size());

        // Create table on node 0.
        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "tbl1").columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT32).asNullable(true).build()
        ).withPrimaryKey("key").build();

        clusterNodes.get(0).tables().createTable(schTbl1.canonicalName(), tblCh ->
                SchemaConfigurationConverter.convert(schTbl1, tblCh)
                        .changeReplicas(5)
                        .changePartitions(1)
        );

        // Put data on node 1.
        Table tbl1 = clusterNodes.get(1).tables().table(schTbl1.canonicalName());
        RecordView<Tuple> recView1 = tbl1.recordView();

        recView1.insert(null, Tuple.create().set("key", 1L).set("val", 111));

        Ignite metaStoreNode = clusterNodes.get(0);

        // Start 2 new nodes after
        Ignite node3 = startNode(testInfo);

        Ignite node4 = startNode(testInfo);

        // Update baseline to nodes 1,4,5
        metaStoreNode.setBaseline(Set.of(metaStoreNode.name(), node3.name(), node4.name()));

        IgnitionManager.stop(clusterNodes.get(1).name());
        IgnitionManager.stop(clusterNodes.get(2).name());

        Table tbl4 = node4.tables().table(schTbl1.canonicalName());

        Tuple keyTuple1 = Tuple.create().set("key", 1L);

        assertEquals(1, (Long) tbl4.recordView().get(null, keyTuple1).value("key"));
    }

    private static String buildConfig(int nodeIdx) {
        return "{\n"
                + "  network: {\n"
                + "    port: " + (BASE_PORT + nodeIdx) + ",\n"
                + "    nodeFinder: {\n"
                + "      netClusterNodes: [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ] \n"
                + "    }\n"
                + "  }\n"
                + "}";
    }

    private Ignite startNode(TestInfo testInfo) {
        CompletableFuture<Ignite> future = startNodeAsync(testInfo, clusterNodes.size());

        assertThat(future, willCompleteSuccessfully());

        Ignite ignite = future.join();

        clusterNodes.add(ignite);

        return ignite;
    }

    private CompletableFuture<Ignite> startNodeAsync(TestInfo testInfo, int index) {
        String nodeName = testNodeName(testInfo, BASE_PORT + index);

        clusterNodeNames.add(nodeName);

        return IgnitionManager.start(nodeName, buildConfig(index), workDir.resolve(nodeName));
    }
}
