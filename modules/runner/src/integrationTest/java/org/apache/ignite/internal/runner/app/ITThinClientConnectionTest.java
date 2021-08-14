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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import com.google.common.collect.Lists;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.app.IgnitionManager;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.SchemaTable;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests thin client connecting to a real server node.
 */
public class ITThinClientConnectionTest extends IgniteAbstractTest {
    /** */
    private static final String SCHEMA_NAME = "PUB";

    /** */
    private static final String TABLE_NAME = "tbl1";

    /** */
    private final String KEY = "key";

    /** */
    private final String VAL = "val";

    /** Nodes bootstrap configuration. */
    private static final Map<String, String> nodesBootstrapCfg = new LinkedHashMap<>() {{
        put("node0", "{\n" +
                "  \"node\": {\n" +
                "    \"metastorageNodes\":[ \"node0\" ]\n" +
                "  },\n" +
                "  \"network\": {\n" +
                "    \"port\":3344,\n" +
                "    \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\" ]\n" +
                "  }\n" +
                "}");

        put("node1", "{\n" +
                "  \"node\": {\n" +
                "    \"metastorageNodes\":[ \"node0\" ]\n" +
                "  },\n" +
                "  \"network\": {\n" +
                "    \"port\":3345,\n" +
                "    \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\" ]\n" +
                "  }\n" +
                "}");
        }};

    /** */
    private final List<Ignite> startedNodes = new ArrayList<>();

    /** */
    @BeforeEach
    @Override public void setup(TestInfo testInfo, @WorkDirectory Path workDir) throws Exception {
        super.setup(testInfo, workDir);

        nodesBootstrapCfg.forEach((nodeName, configStr) ->
                startedNodes.add(IgnitionManager.start(nodeName, configStr, workDir.resolve(nodeName)))
        );

        SchemaTable schTbl = SchemaBuilders.tableBuilder(SCHEMA_NAME, TABLE_NAME).columns(
                SchemaBuilders.column(KEY, ColumnType.INT32).asNonNull().build(),
                SchemaBuilders.column(VAL, ColumnType.string()).asNullable().build()
        ).withPrimaryKey(KEY).build();

        startedNodes.get(0).tables().createTable(schTbl.canonicalName(), tblCh ->
                SchemaConfigurationConverter.convert(schTbl, tblCh)
                        .changeReplicas(1)
                        .changePartitions(10)
        );
    }

    /** */
    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(Lists.reverse(startedNodes));
    }

    /**
     * Check that thin client can connect to any server node and work with table API.
     */
    @Test
    void testThinClientConnectsToServerNodesAndExecutesBasicTableOperations() throws Exception {
        for (var addr : new String[] {"127.0.0.1:10800", "127.0.0.1:10801"}) {
            try (var client = IgniteClient.builder().addresses(addr).build()) {
                List<Table> tables = client.tables().tables();
                assertEquals(1, tables.size());

                Table table = tables.get(0);
                assertEquals(String.format("%s.%s", SCHEMA_NAME, TABLE_NAME), table.tableName());

                var tuple = table.tupleBuilder().set(KEY, 1).set(VAL, "Hello").build();
                var keyTuple = table.tupleBuilder().set(KEY, 1).build();

                table.upsert(tuple);
                assertEquals("Hello", table.get(keyTuple).stringValue(VAL));

                assertTrue(table.delete(keyTuple));
            }
        }
    }
}
