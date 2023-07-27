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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.schema.testutils.SchemaConfigurationConverter.convert;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders;
import org.apache.ignite.internal.schema.testutils.definition.ColumnType;
import org.apache.ignite.internal.sql.engine.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.TransactionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * There are tests which check a table managment contract.
 * <ul>
 * <li>When a table is already created other tries to create the table have to fail {@link TableAlreadyExistsException}.</li>
 * <li>When a table is not existed, tries to alter or drop the table have to failed {@link TableNotFoundException}.</li>
 * </ul>
 */
public class ItTableApiContractTest extends ClusterPerClassIntegrationTest {
    /** Schema name. */
    public static final String SCHEMA = "PUBLIC";

    /** Table name. */
    public static final String TABLE_NAME = "TBL1";

    /** Cluster nodes. */
    private static Ignite ignite;

    /** {@inheritDoc} */
    @Override
    public int nodes() {
        return 1;
    }

    /**
     * Before all tests.
     */
    @BeforeAll
    static void beforeAll() {
        ignite = CLUSTER_NODES.get(0);
    }

    /**
     * Executes after each test.
     */
    @AfterEach
    void afterTest() {
        sql("DROP TABLE IF EXISTS " + TABLE_NAME);
    }

    /**
     * Executes before test.
     */
    @BeforeEach
    void beforeTest() {
        sql("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (name VARCHAR PRIMARY KEY, balance INT NOT NULL)");
    }

    /**
     * The test invokes various API methods on KeyValue and Record views when key is locked.
     * The expected behavior all the invocations lead to a transaction exception due to the key is locked.
     */
    @Test
    public void tableTransactionExceptionContract() {
        KeyValueView<String, Integer> kv = ignite.tables().table(TABLE_NAME).keyValueView(String.class, Integer.class);

        var tx = ignite.transactions().begin();

        kv.put(tx, "k1", 1);

        assertThrowsExactly(TransactionException.class, () -> kv.put(null, "k1", 2));
        assertThat(kv.getAsync(null, "k1"), willCompleteSuccessfully());
        assertThrowsExactly(TransactionException.class, () -> kv.remove(null, "k1"));
        assertThrowsExactly(TransactionException.class, () -> kv.remove(null, "k1", 1));
        assertThat(kv.containsAsync(null, "k1"), willCompleteSuccessfully());
        assertThrowsExactly(TransactionException.class, () -> kv.replace(null, "k1", 2));

        tx.rollback();

        RecordView<Tuple> recordView = ignite.tables().table(TABLE_NAME).recordView();

        tx = ignite.transactions().begin();

        recordView.insert(tx, Tuple.create().set("name", "k1").set("balance", 1));

        assertThrowsExactly(TransactionException.class, () -> recordView.insert(null, Tuple.create().set("name", "k1").set("balance", 2)));
        assertThrowsExactly(TransactionException.class, () -> recordView.upsert(null, Tuple.create().set("name", "k1").set("balance", 2)));
        assertThat(recordView.getAsync(null, Tuple.create().set("name", "k1")), willCompleteSuccessfully());
        assertThrowsExactly(TransactionException.class, () -> recordView.delete(null, Tuple.create().set("name", "k1")));
        assertThrowsExactly(TransactionException.class,
                () -> recordView.deleteExact(null, Tuple.create().set("name", "k1").set("balance", 1)));
        assertThrowsExactly(TransactionException.class, () -> recordView.replace(null, Tuple.create().set("name", "k1").set("balance", 2)));

        tx.rollback();
    }

    /**
     * Checks a contract for asynchronous dropping table.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDropTable() throws Exception {
        CompletableFuture<Void> dropTblFut1 =  tableManager().dropTableAsync(TABLE_NAME);

        dropTblFut1.get();

        assertNull(ignite.tables().table(TABLE_NAME));

        CompletableFuture<Void> dropTblFut2 = tableManager().dropTableAsync(TABLE_NAME);

        assertThrows(TableNotFoundException.class, () -> futureResult(dropTblFut2));
    }

    /**
     * Checks a contract for altering table.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAlterTable() throws Exception {
        await(tableManager().alterTableAsync(TABLE_NAME, chng -> {
            chng.changeColumns(cols ->
                    cols.create("NAME_1", colChg -> convert(SchemaBuilders.column("NAME_1", ColumnType.string()).asNullable(true)
                            .withDefaultValue("default").build(), colChg)));
            return true;
        }));

        assertNotNull(ignite.tables().table(TABLE_NAME));

        assertNull(ignite.tables().table(TABLE_NAME + "_not_exist"));

        assertThrows(TableNotFoundException.class, () -> await(tableManager().alterTableAsync(TABLE_NAME + "_not_exist", chng -> {
            chng.changeColumns(cols ->
                    cols.create("NAME_1", colChg -> convert(SchemaBuilders.column("NAME_1", ColumnType.string()).asNullable(true)
                            .withDefaultValue("default").build(), colChg)));
            return true;
        })));
    }

    /**
     * Checks a contract for asynchronous altering table.
     *
     * @throws Exception If fialed.
     */
    @Test
    public void testAlterTableAsync() throws Exception {
        CompletableFuture<Void> altTblFut1 = tableManager().alterTableAsync(TABLE_NAME,
                chng -> {
                    chng.changeColumns(cols ->
                            cols.create("NAME_1", colChg -> convert(SchemaBuilders.column("NAME_1",
                                            ColumnType.string()).asNullable(true).withDefaultValue("default").build(), colChg)));
                return true;
            });

        CompletableFuture<Void> altTblFut2 = tableManager().alterTableAsync(TABLE_NAME + "_not_exist",
                chng -> {
                    chng.changeColumns(cols ->
                            cols.create("NAME_1", colChg -> convert(SchemaBuilders.column("NAME_1",
                                            ColumnType.string()).asNullable(true).withDefaultValue("default").build(), colChg)));
                    return true;
            });

        assertNotNull(ignite.tables().table(TABLE_NAME));

        assertNull(ignite.tables().table(TABLE_NAME + "_not_exist"));

        altTblFut1.get();

        assertThrows(TableNotFoundException.class, () -> futureResult(altTblFut2));
    }

    /**
     * Checks a contract for table creation.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateTable() throws Exception {
        Table table = ignite.tables().table(TABLE_NAME);

        assertNotNull(table);

        assertThrows(TableAlreadyExistsException.class,
                () -> await(tableManager().createTableAsync(TABLE_NAME, DEFAULT_ZONE_NAME,
                        tableChange -> convert(SchemaBuilders.tableBuilder(SCHEMA, TABLE_NAME)
                                .columns(
                                        SchemaBuilders.column("new_key", ColumnType.INT64).build(),
                                        SchemaBuilders.column("new_val", ColumnType.string()).build())
                                .withPrimaryKey("new_key")
                                .build(), tableChange))));
    }

    /**
     * Checks a contract for asynchronous table creation.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateTableAsync() throws Exception {
        assertNotNull(ignite.tables().table(TABLE_NAME));

        CompletableFuture<Table> tableFut2 = tableManager()
                .createTableAsync(TABLE_NAME, DEFAULT_ZONE_NAME, tableChange -> convert(SchemaBuilders.tableBuilder(SCHEMA, TABLE_NAME)
                        .columns(
                                SchemaBuilders.column("new_key", ColumnType.INT64).build(),
                                SchemaBuilders.column("new_val", ColumnType.string()).build())
                        .withPrimaryKey("new_key")
                        .build(), tableChange));

        assertThrows(TableAlreadyExistsException.class, () -> futureResult(tableFut2));
    }

    @Test
    public void testGetAll() {
        RecordView<Tuple> tbl = ignite.tables().table(TABLE_NAME).recordView();

        assertThat(
                tbl.getAll(null, List.of(Tuple.create().set("name", "id_0"))),
                contains(nullValue())
        );

        var recs = IntStream.range(0, 5)
                .mapToObj(i -> Tuple.create().set("name", "id_" + i * 2).set("balance", i * 2))
                .collect(toList());

        tbl.upsertAll(null, recs);

        var keys = IntStream.range(0, 10)
                .mapToObj(i -> Tuple.create().set("name", "id_" + i))
                .collect(toList());

        List<Tuple> res = tbl.getAll(null, keys);

        assertThat(
                res.stream().map(tuple -> tuple == null ? null : tuple.stringValue(0)).collect(toList()),
                contains("id_0", null, "id_2", null, "id_4", null, "id_6", null, "id_8", null)
        );
    }

    private TableManager tableManager() {
        return (TableManager) ignite.tables();
    }

    /**
     * Gets future result and unwrap exception if it was thrown.
     *
     * @param fut Some future.
     * @param <T> Expected future result parameter.
     * @return Future result.
     * @throws Throwable If future completed with an exception.
     */
    private <T> T futureResult(CompletableFuture<T> fut) throws Throwable {
        try {
            return fut.get();
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }
}
