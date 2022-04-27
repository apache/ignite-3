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

package org.apache.ignite.internal.sql.engine;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.sql.engine.util.CursorUtils.getAllFromCursor;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.schema.definition.builder.TableDefinitionBuilder;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Abstract basic integration test.
 */
@ExtendWith(WorkDirectoryExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AbstractBasicIntegrationTest extends BaseIgniteAbstractTest {
    private static final IgniteLogger LOG = IgniteLogger.forClass(AbstractBasicIntegrationTest.class);

    /** Base port number. */
    private static final int BASE_PORT = 3344;

    /** Nodes bootstrap configuration pattern. */
    private static final String NODE_BOOTSTRAP_CFG = "{\n"
            + "  \"network\": {\n"
            + "    \"port\":{},\n"
            + "    \"nodeFinder\":{\n"
            + "      \"netClusterNodes\": [ {} ]\n"
            + "    }\n"
            + "  }\n"
            + "}";

    /** Cluster nodes. */
    protected static final List<Ignite> CLUSTER_NODES = new ArrayList<>();

    /** Work directory. */
    @WorkDirectory
    private static Path WORK_DIR;

    /**
     * Before all.
     *
     * @param testInfo Test information object.
     */
    @BeforeAll
    void startNodes(TestInfo testInfo) throws Exception {
        String connectNodeAddr = "\"localhost:" + BASE_PORT + '\"';

        List<CompletableFuture<Ignite>> futures = new ArrayList<>();

        for (int i = 0; i < nodes(); i++) {
            String nodeName = testNodeName(testInfo, i);

            String config = IgniteStringFormatter.format(NODE_BOOTSTRAP_CFG, BASE_PORT + i, connectNodeAddr);

            futures.add(IgnitionManager.start(nodeName, config, WORK_DIR.resolve(nodeName)));
        }

        String metaStorageNodeName = testNodeName(testInfo, 0);

        IgnitionManager.init(metaStorageNodeName, List.of(metaStorageNodeName));

        for (CompletableFuture<Ignite> future : futures) {
            assertThat(future, willCompleteSuccessfully());

            CLUSTER_NODES.add(future.join());
        }
    }

    /**
     * Get a count of nodes in the Ignite cluster.
     *
     * @return Count of nodes.
     */
    protected int nodes() {
        return 3;
    }

    /**
     * After all.
     */
    @AfterAll
    void stopNodes(TestInfo testInfo) throws Exception {
        LOG.info("Start tearDown()");

        List<AutoCloseable> closeables = IntStream.range(0, nodes())
                .mapToObj(i -> testNodeName(testInfo, i))
                .map(nodeName -> (AutoCloseable) () -> IgnitionManager.stop(nodeName))
                .collect(toList());

        IgniteUtils.closeAll(closeables);

        CLUSTER_NODES.clear();

        LOG.info("End tearDown()");
    }

    /** Drops all visible tables. */
    protected void dropAllTables() {
        for (Table t : CLUSTER_NODES.get(0).tables().tables()) {
            sql("DROP TABLE " + t.name());
        }
    }

    /**
     * Invokes before the test will start.
     *
     * @param testInfo Test information object.
     * @throws Exception If failed.
     */
    @BeforeEach
    public void setup(TestInfo testInfo) throws Exception {
        setupBase(testInfo, WORK_DIR);
    }

    /**
     * Invokes after the test has finished.
     *
     * @param testInfo Test information oject.
     * @throws Exception If failed.
     */
    @AfterEach
    public void tearDown(TestInfo testInfo) throws Exception {
        tearDownBase(testInfo);
    }

    protected static QueryChecker assertQuery(String qry) {
        return new QueryChecker(qry) {
            @Override
            protected QueryProcessor getEngine() {
                return ((IgniteImpl) CLUSTER_NODES.get(0)).queryEngine();
            }
        };
    }

    protected static Table createAndPopulateTable() {
        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "PERSON").columns(
                SchemaBuilders.column("ID", ColumnType.INT32).build(),
                SchemaBuilders.column("NAME", ColumnType.string()).asNullable(true).build(),
                SchemaBuilders.column("SALARY", ColumnType.DOUBLE).asNullable(true).build()
        ).withPrimaryKey("ID").build();

        Table tbl = CLUSTER_NODES.get(0).tables().createTable(schTbl1.canonicalName(), tblCh ->
                SchemaConfigurationConverter.convert(schTbl1, tblCh)
                        .changeReplicas(1)
                        .changePartitions(10)
        );

        int idx = 0;

        insertData(tbl, new String[]{"ID", "NAME", "SALARY"}, new Object[][]{
                {idx++, "Igor", 10d},
                {idx++, null, 15d},
                {idx++, "Ilya", 15d},
                {idx++, "Roma", 10d},
                {idx, "Roma", 10d}
        });

        return tbl;
    }

    protected static void createTable(TableDefinitionBuilder tblBld) {
        TableDefinition schTbl1 = tblBld.build();

        CLUSTER_NODES.get(0).tables().createTable(schTbl1.canonicalName(), tblCh ->
                SchemaConfigurationConverter.convert(schTbl1, tblCh)
                        .changeReplicas(1)
                        .changePartitions(10)
        );
    }

    protected static Table table(String canonicalName) {
        return CLUSTER_NODES.get(0).tables().table(canonicalName);
    }

    protected static void insertData(String tblName, String[] columnNames, Object[]... tuples) {
        insertData(CLUSTER_NODES.get(0).tables().table(tblName), columnNames, tuples);
    }

    protected static void insertData(Table table, String[] columnNames, Object[]... tuples) {
        RecordView<Tuple> view = table.recordView();

        int batchSize = 128;

        List<Tuple> batch = new ArrayList<>(batchSize);
        for (Object[] tuple : tuples) {
            assert tuple != null && tuple.length == columnNames.length;

            Tuple toInsert = Tuple.create();

            for (int i = 0; i < tuple.length; i++) {
                toInsert.set(columnNames[i], tuple[i]);
            }

            batch.add(toInsert);

            if (batch.size() == batchSize) {
                Collection<Tuple> duplicates = view.insertAll(null, batch);

                if (!duplicates.isEmpty()) {
                    throw new AssertionError("Duplicated rows detected: " + duplicates);
                }

                batch.clear();
            }
        }

        if (!batch.isEmpty()) {
            view.insertAll(null, batch);

            batch.clear();
        }
    }

    protected static void checkData(Table table, String[] columnNames, Object[]... tuples) {
        RecordView<Tuple> view = table.recordView();

        for (Object[] tuple : tuples) {
            assert tuple != null && tuple.length == columnNames.length;

            Object id = tuple[0];

            assert id != null : "Primary key cannot be null";

            Tuple row  = view.get(null, Tuple.create().set(columnNames[0], id));

            assertNotNull(row);

            for (int i = 0; i < columnNames.length; i++) {
                assertEquals(tuple[i], row.value(columnNames[i]));
            }
        }
    }

    protected static List<List<Object>> sql(String sql, Object... args) {
        return getAllFromCursor(
                await(((IgniteImpl) CLUSTER_NODES.get(0)).queryEngine().queryAsync("PUBLIC", sql, args).get(0))
        );
    }
}
