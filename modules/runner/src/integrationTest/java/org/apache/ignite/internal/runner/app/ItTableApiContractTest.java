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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.util.List;
import java.util.concurrent.CompletableFuture;
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
 *     <li>When a table is already created other tries to create the table have to fail {@link TableAlreadyExistsException}.</li>
 *     <li>When a table is not existed, tries to alter or drop the table have to failed {@link TableNotFoundException}.</li>
 * </ul>
 */
public class ItTableApiContractTest extends ClusterPerClassIntegrationTest {
    private static final String SCHEMA_NAME = "PUBLIC";

    public static final String TABLE_NAME = "TBL1";

    private static Ignite ignite;

    @Override
    public int nodes() {
        return 1;
    }

    @BeforeAll
    static void beforeAll() {
        ignite = CLUSTER_NODES.get(0);
    }

    @AfterEach
    void afterTest() {
        sql("DROP TABLE IF EXISTS " + TABLE_NAME);
    }

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
        assertThrowsExactly(TransactionException.class, () -> kv.get(null, "k1"));
        assertThrowsExactly(TransactionException.class, () -> kv.remove(null, "k1"));
        assertThrowsExactly(TransactionException.class, () -> kv.remove(null, "k1", 1));
        assertThrowsExactly(TransactionException.class, () -> kv.contains(null, "k1"));
        assertThrowsExactly(TransactionException.class, () -> kv.replace(null, "k1", 2));

        tx.rollback();

        RecordView<Tuple> recordView = ignite.tables().table(TABLE_NAME).recordView();

        tx = ignite.transactions().begin();

        recordView.insert(tx, Tuple.create().set("name", "k1").set("balance", 1));

        assertThrowsExactly(TransactionException.class, () -> recordView.insert(null, Tuple.create().set("name", "k1").set("balance", 2)));
        assertThrowsExactly(TransactionException.class, () -> recordView.upsert(null, Tuple.create().set("name", "k1").set("balance", 2)));
        assertThrowsExactly(TransactionException.class, () -> recordView.get(null, Tuple.create().set("name", "k1")));
        assertThrowsExactly(TransactionException.class, () -> recordView.delete(null, Tuple.create().set("name", "k1")));
        assertThrowsExactly(TransactionException.class,
                () -> recordView.deleteExact(null, Tuple.create().set("name", "k1").set("balance", 1)));
        assertThrowsExactly(TransactionException.class, () -> recordView.replace(null, Tuple.create().set("name", "k1").set("balance", 2)));

        tx.rollback();
    }

    /**
     * Checks a contract for asynchronous dropping table.
     */
    @Test
    public void testDropTable() {
        assertThat(tableManager().dropTableAsync(TABLE_NAME), willSucceedFast());

        assertNull(ignite.tables().table(TABLE_NAME));

        assertThat(tableManager().dropTableAsync(TABLE_NAME), willThrowFast(TableNotFoundException.class));
    }

    /**
     * Checks a contract for altering table.
     */
    @Test
    public void testAlterTable() {
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
     */
    @Test
    public void testAlterTableAsync() {
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

        assertThat(altTblFut1, willSucceedFast());
        assertThat(altTblFut2, willThrowFast(TableNotFoundException.class));
    }

    /**
     * Checks a contract for table creation.
     */
    @Test
    public void testCreateTable() {
        Table table = ignite.tables().table(TABLE_NAME);

        assertNotNull(table);

        assertThrows(TableAlreadyExistsException.class,
                () -> await(tableManager().createTableAsync(TABLE_NAME, DEFAULT_ZONE_NAME,
                        tableChange -> convert(SchemaBuilders.tableBuilder(SCHEMA_NAME, TABLE_NAME)
                                .columns(
                                        SchemaBuilders.column("new_key", ColumnType.INT64).build(),
                                        SchemaBuilders.column("new_val", ColumnType.string()).build())
                                .withPrimaryKey("new_key")
                                .build(), tableChange))));
    }

    /**
     * Checks a contract for asynchronous table creation.
     */
    @Test
    public void testCreateTableAsync() {
        assertNotNull(ignite.tables().table(TABLE_NAME));

        CompletableFuture<Table> tableFut2 = tableManager()
                .createTableAsync(TABLE_NAME, DEFAULT_ZONE_NAME, tableChange -> convert(SchemaBuilders.tableBuilder(SCHEMA_NAME, TABLE_NAME)
                        .columns(
                                SchemaBuilders.column("new_key", ColumnType.INT64).build(),
                                SchemaBuilders.column("new_val", ColumnType.string()).build())
                        .withPrimaryKey("new_key")
                        .build(), tableChange));

        assertThat(tableFut2, willThrowFast(TableAlreadyExistsException.class));
    }

    @Test
    public void testGetAll() {
        RecordView<Tuple> tbl = ignite.tables().table(TABLE_NAME).recordView();

        List<Tuple> recs = IntStream.range(1, 50)
                .mapToObj(i -> Tuple.create().set("name", "id_" + i * 2).set("balance", i * 2))
                .collect(toList());

        tbl.upsertAll(null, recs);

        List<Tuple> keyRecs = IntStream.range(1, 100)
                .mapToObj(i -> Tuple.create().set("name", "id_" + i))
                .collect(toList());

        List<Tuple> res = tbl.getAll(null, keyRecs);

        assertThat(res, hasSize(keyRecs.size()));

        assertNull(res.get(0));
        assertEquals(2L, res.get(1).longValue(0));
    }

    private static TableManager tableManager() {
        return (TableManager) ignite.tables();
    }
}
