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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.ItUtils;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test for baseline changes.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-16471")
@ExtendWith(WorkDirectoryExtension.class)
public class ItBaselineChangesTest {
    /** Start network port for test nodes. */
    private static final int BASE_PORT = 3344;

    /** Nodes bootstrap configuration. */
    private final Map<String, String> initClusterNodes = new LinkedHashMap<>();

    private final List<Ignite> clusterNodes = new ArrayList<>();

    @WorkDirectory
    private Path workDir;

    /**
     * Before each.
     */
    @BeforeEach
    void setUp(TestInfo testInfo) {
        for (int i = 0; i < 3; ++i) {
            String nodeName = testNodeName(testInfo, BASE_PORT + i);

            initClusterNodes.put(nodeName, buildConfig(i));
        }
    }

    /**
     * After each.
     */
    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(ItUtils.reverse(clusterNodes));
    }

    /**
     * Check dynamic table creation.
     */
    @Test
    void testBaselineExtending(TestInfo testInfo) throws NodeStoppingException {
        initClusterNodes.forEach((nodeName, configStr) ->
                clusterNodes.add(IgnitionManager.start(nodeName, configStr, workDir.resolve(nodeName)))
        );

        assertEquals(3, clusterNodes.size());

        IgniteImpl metastorageNode = (IgniteImpl) clusterNodes.get(0);

        metastorageNode.init(List.of(metastorageNode.name()));

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

        var metaStoreNode = clusterNodes.get(0);

        var node3Name = testNodeName(testInfo, nodePort(3));
        var node4Name = testNodeName(testInfo, nodePort(4));

        // Start 2 new nodes after
        var node3 = IgnitionManager.start(node3Name, buildConfig(3), workDir.resolve(node3Name));

        clusterNodes.add(node3);

        var node4 = IgnitionManager.start(node4Name, buildConfig(4), workDir.resolve(node4Name));

        clusterNodes.add(node4);

        // Update baseline to nodes 1,4,5
        metaStoreNode.setBaseline(Set.of(metaStoreNode.name(), node3Name, node4Name));

        IgnitionManager.stop(clusterNodes.get(1).name());
        IgnitionManager.stop(clusterNodes.get(2).name());

        Table tbl4 = node4.tables().table(schTbl1.canonicalName());

        Tuple keyTuple1 = Tuple.create().set("key", 1L);

        assertEquals(1, (Long) tbl4.recordView().get(null, keyTuple1).value("key"));
    }

    private static String buildConfig(int nodeIdx) {
        return "{\n"
                + "  network: {\n"
                + "    port: " + nodePort(nodeIdx) + ",\n"
                + "    nodeFinder: {\n"
                + "      netClusterNodes: [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ] \n"
                + "    }\n"
                + "  }\n"
                + "}";
    }

    private static int nodePort(int nodeIdx) {
        return BASE_PORT + nodeIdx;
    }
}
