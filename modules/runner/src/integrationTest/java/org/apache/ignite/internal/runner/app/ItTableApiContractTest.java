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

import static org.apache.ignite.internal.schema.testutils.SchemaConfigurationConverter.convert;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders;
import org.apache.ignite.internal.schema.testutils.definition.ColumnType;
import org.apache.ignite.internal.sql.engine.AbstractBasicIntegrationTest;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * There are tests which check a table managment contract.
 * <ul>
 * <li>When a table is already created other tries to create the table have to fail {@link TableAlreadyExistsException}.</li>
 * <li>When a table is not existed, tries to alter or drop the table have to failed {@link TableNotFoundException}.</li>
 * </ul>
 */
public class ItTableApiContractTest extends AbstractBasicIntegrationTest {
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
        if (ignite.tables().table(TABLE_NAME) != null) {
            await(tableManager().dropTableAsync(TABLE_NAME));
        }
    }

    /**
     * Checks a contract for asynchronous dropping table.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDropTable() throws Exception {
        await(tableManager().createTableAsync(TABLE_NAME, tableChange -> convert(SchemaBuilders.tableBuilder(SCHEMA, TABLE_NAME)
                .columns(
                        SchemaBuilders.column("key", ColumnType.INT64).build(),
                        SchemaBuilders.column("val", ColumnType.string()).build())
                .withPrimaryKey("key")
                .build(), tableChange)
                .changeReplicas(2)
                .changePartitions(10)));

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
        await(tableManager().createTableAsync(TABLE_NAME, tableChange -> convert(SchemaBuilders.tableBuilder(SCHEMA, TABLE_NAME)
                .columns(
                        SchemaBuilders.column("key", ColumnType.INT64).build(),
                        SchemaBuilders.column("val", ColumnType.string()).build())
                .withPrimaryKey("key")
                .build(), tableChange)
                .changeReplicas(2)
                .changePartitions(10)));

        await(tableManager().alterTableAsync(TABLE_NAME, chng -> {
            chng.changeColumns(cols ->
                    cols.create("NAME", colChg -> convert(SchemaBuilders.column("name", ColumnType.string()).asNullable(true)
                            .withDefaultValue("default").build(), colChg)));
            return true;
        }));

        assertNotNull(ignite.tables().table(TABLE_NAME));

        assertNull(ignite.tables().table(TABLE_NAME + "_not_exist"));

        assertThrows(TableNotFoundException.class, () -> await(tableManager().alterTableAsync(TABLE_NAME + "_not_exist", chng -> {
            chng.changeColumns(cols ->
                    cols.create("NAME", colChg -> convert(SchemaBuilders.column("name", ColumnType.string()).asNullable(true)
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
        await(tableManager().createTableAsync(TABLE_NAME, tableChange -> convert(SchemaBuilders.tableBuilder(SCHEMA, TABLE_NAME)
                .columns(
                        SchemaBuilders.column("key", ColumnType.INT64).build(),
                        SchemaBuilders.column("val", ColumnType.string()).build())
                .withPrimaryKey("key")
                .build(), tableChange)
                .changeReplicas(2)
                .changePartitions(10)));

        CompletableFuture<Void> altTblFut1 = tableManager().alterTableAsync(TABLE_NAME,
                chng -> {
                    chng.changeColumns(cols ->
                            cols.create("NAME", colChg -> convert(SchemaBuilders.column("NAME",
                                            ColumnType.string()).asNullable(true).withDefaultValue("default").build(), colChg)));
                return true;
            });

        CompletableFuture<Void> altTblFut2 = tableManager().alterTableAsync(TABLE_NAME + "_not_exist",
                chng -> {
                    chng.changeColumns(cols ->
                            cols.create("NAME", colChg -> convert(SchemaBuilders.column("NAME",
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
        Table table = await(tableManager().createTableAsync(TABLE_NAME,
                tableChange -> convert(SchemaBuilders.tableBuilder(SCHEMA, TABLE_NAME)
                        .columns(
                                SchemaBuilders.column("key", ColumnType.INT64).build(),
                                SchemaBuilders.column("val", ColumnType.string()).build())
                        .withPrimaryKey("key")
                        .build(), tableChange)
                        .changeReplicas(2)
                        .changePartitions(10)));

        assertNotNull(table);

        assertThrows(TableAlreadyExistsException.class,
                () -> await(tableManager().createTableAsync(TABLE_NAME,
                        tableChange -> convert(SchemaBuilders.tableBuilder(SCHEMA, TABLE_NAME)
                                .columns(
                                        SchemaBuilders.column("new_key", ColumnType.INT64).build(),
                                        SchemaBuilders.column("new_val", ColumnType.string()).build())
                                .withPrimaryKey("new_key")
                                .build(), tableChange)
                                .changeReplicas(2)
                                .changePartitions(10))));
    }

    /**
     * Checks a contract for asynchronous table creation.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateTableAsync() throws Exception {
        CompletableFuture<Table> tableFut1 = tableManager()
                .createTableAsync(TABLE_NAME, tableChange -> convert(SchemaBuilders.tableBuilder(SCHEMA, TABLE_NAME)
                        .columns(
                                SchemaBuilders.column("key", ColumnType.INT64).build(),
                                SchemaBuilders.column("val", ColumnType.string()).build())
                        .withPrimaryKey("key")
                        .build(), tableChange)
                        .changeReplicas(2)
                        .changePartitions(10));

        assertNotNull(tableFut1.get());

        CompletableFuture<Table> tableFut2 = tableManager()
                .createTableAsync(TABLE_NAME, tableChange -> convert(SchemaBuilders.tableBuilder(SCHEMA, TABLE_NAME)
                        .columns(
                                SchemaBuilders.column("new_key", ColumnType.INT64).build(),
                                SchemaBuilders.column("new_val", ColumnType.string()).build())
                        .withPrimaryKey("new_key")
                        .build(), tableChange)
                        .changeReplicas(2)
                        .changePartitions(10));

        assertThrows(TableAlreadyExistsException.class, () -> futureResult(tableFut2));
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
