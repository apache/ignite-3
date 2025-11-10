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

package org.apache.ignite.internal.sql.engine.exec;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.exec.fsm.QueryInfo;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.table.QualifiedName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests cases for batched ddl processing. */
public class DdlBatchingTest extends BaseIgniteAbstractTest {
    private static final String NODE_NAME = "gateway";

    private final AtomicInteger executeCallCounter = new AtomicInteger();

    private TestCluster cluster;
    private TestNode gatewayNode;

    @BeforeEach
    void startCluster() {
        cluster = TestBuilders.cluster()
                .nodes(NODE_NAME)
                .catalogManagerDecorator(this::catalogManagerDecorator)
                .build();

        cluster.start();

        gatewayNode = cluster.node(NODE_NAME);

        executeCallCounter.set(0);
    }

    @AfterEach
    void stopCluster() throws Exception {
        assertTrue(
                waitForCondition(() -> gatewayNode.runningQueries().isEmpty(), 2_000),
                "Some queries are stuck in registry: \n" + gatewayNode.runningQueries().stream()
                        .map(QueryInfo::toString)
                        .collect(Collectors.joining(
                                "," + System.lineSeparator() + "\t",
                                "[" + System.lineSeparator() + "\t",
                                System.lineSeparator() + "]"))
        );

        cluster.stop();
    }

    @Test
    void schemaAndTableCreatedInTheSameBatch() {
        AsyncSqlCursor<InternalSqlRow> cursor = gatewayNode.executeQuery(
                "CREATE SCHEMA my_schema;"
                        + "CREATE TABLE my_schema.t1 (id INT PRIMARY KEY, val_1 INT, val_2 INT);"
                        + "CREATE INDEX t1_ind_1 ON my_schema.t1 (val_1);"
        );

        // CREATE SCHEMA my_schema
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willSucceedFast());

        // CREATE TABLE my_schema.t1 (id INT PRIMARY KEY, val_1 INT, val_2 INT)
        cursor = cursor.nextResult().join();
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willSucceedFast());

        // CREATE INDEX t1_ind_1 ON my_schema.t1 (val_1)
        cursor = cursor.nextResult().join();
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(false));

        assertEquals(1, executeCallCounter.get());

        assertSchemaExists("my_schema");
        assertTableExists(QualifiedName.of("my_schema", "t1"));
        assertIndexExists(QualifiedName.of("my_schema", "t1_ind_1"));
    }

    @Test
    void fewDdlAreBatched() {
        AsyncSqlCursor<InternalSqlRow> cursor = gatewayNode.executeQuery(
                "CREATE TABLE t1 (id INT PRIMARY KEY, val_1 INT, val_2 INT);"
                + "CREATE INDEX t1_ind_1 ON t1 (val_1);"
                + "CREATE INDEX t1_ind_2 ON t1 (val_2);"
        );

        // CREATE TABLE t1 (id INT PRIMARY KEY, val_1 INT, val_2 INT)
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willSucceedFast());

        // CREATE INDEX t1_ind_1 ON t1 (val_1)
        cursor = cursor.nextResult().join();
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willSucceedFast());

        // CREATE INDEX t1_ind_2 ON t1 (val_2)
        cursor = cursor.nextResult().join();
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(false));

        assertEquals(1, executeCallCounter.get());

        assertTableExists("t1");
        assertIndexExists("t1_ind_1");
        assertIndexExists("t1_ind_2");
    }

    @Test
    void batchWithConditionalDdl() {
        AsyncSqlCursor<InternalSqlRow> cursor = gatewayNode.executeQuery(
                "CREATE TABLE IF NOT EXISTS t1 (id INT PRIMARY KEY, val_1 INT, val_2 INT);"
                        + "CREATE INDEX IF NOT EXISTS t1_ind_1 ON t1 (val_1);"
                        + "CREATE TABLE IF NOT EXISTS t1 (id INT PRIMARY KEY, val_1 INT, val_2 INT);"
                        + "CREATE INDEX IF NOT EXISTS t1_ind_1 ON t1 (val_1);"
                        + "CREATE INDEX t1_ind_2 ON t1 (val_2);"
        );

        // CREATE TABLE IF NOT EXISTS t1 (id INT PRIMARY KEY, val_1 INT, val_2 INT)
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willSucceedFast());

        // CREATE INDEX IF NOT EXISTS t1_ind_1 ON t1 (val_1)
        cursor = cursor.nextResult().join();
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willSucceedFast());

        // CREATE TABLE IF NOT EXISTS t1 (id INT PRIMARY KEY, val_1 INT, val_2 INT)
        cursor = cursor.nextResult().join();
        assertDdlResult(cursor, false);
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willSucceedFast());

        // CREATE INDEX IF NOT EXISTS t1_ind_1 ON t1 (val_1)
        cursor = cursor.nextResult().join();
        assertDdlResult(cursor, false);
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willSucceedFast());

        // CREATE INDEX t1_ind_2 ON t1 (val_2)
        cursor = cursor.nextResult().join();
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(false));

        assertEquals(1, executeCallCounter.get());

        assertTableExists("t1");
        assertIndexExists("t1_ind_1");
        assertIndexExists("t1_ind_2");
    }

    @Test
    void batchIsSplitByAlter() {
        AsyncSqlCursor<InternalSqlRow> cursor = gatewayNode.executeQuery(
                "CREATE TABLE t1 (id INT PRIMARY KEY, val_1 INT, val_2 INT);"
                        + "ALTER TABLE t1 ADD COLUMN val_3 INT;"
                        + "ALTER TABLE t1 DROP COLUMN val_2;"
                        + "CREATE TABLE t2 (id INT PRIMARY KEY, val_1 INT, val_2 INT);"
        );

        // CREATE TABLE t1 (id INT PRIMARY KEY, val_1 INT, val_2 INT)
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willSucceedFast());

        // ALTER TABLE t ADD COLUMN val_3 INT;
        cursor = cursor.nextResult().join();
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willSucceedFast());

        // ALTER TABLE t DROP COLUMN val_2 INT;
        cursor = cursor.nextResult().join();
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willSucceedFast());

        // CREATE TABLE t2 (id INT PRIMARY KEY, val_1 INT, val_2 INT)
        cursor = cursor.nextResult().join();
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(false));

        // ALTER splits the batch
        assertEquals(4, executeCallCounter.get());

        assertTableExists("t1");
        assertTableExists("t2");
    }

    @Test
    void batchIsSplitByDrop() {
        AsyncSqlCursor<InternalSqlRow> cursor = gatewayNode.executeQuery(
                "CREATE TABLE t1 (id INT PRIMARY KEY, val_1 INT, val_2 INT);"
                        + "CREATE TABLE t2 (id INT PRIMARY KEY, val_1 INT, val_2 INT);"
                        + "CREATE INDEX t1_ind_1 ON t1 (val_1);"
                        + "DROP TABLE t1;"
                        + "DROP TABLE t2;"
                        + "CREATE TABLE t1 (id INT PRIMARY KEY, val_1 INT, val_2 INT);"
        );

        // CREATE TABLE t1 (id INT PRIMARY KEY, val_1 INT, val_2 INT)
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willSucceedFast());

        // CREATE TABLE t2 (id INT PRIMARY KEY, val_1 INT, val_2 INT)
        cursor = cursor.nextResult().join();
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willSucceedFast());

        // CREATE INDEX t1_ind_1 ON t1 (val_1)
        cursor = cursor.nextResult().join();
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willSucceedFast());

        // DROP TABLE t1
        cursor = cursor.nextResult().join();
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willSucceedFast());

        // DROP TABLE t2
        cursor = cursor.nextResult().join();
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willSucceedFast());

        // CREATE TABLE t1 (id INT PRIMARY KEY, val_1 INT, val_2 INT)
        cursor = cursor.nextResult().join();
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(false));

        assertEquals(2, executeCallCounter.get());
        assertTableExists("t1");
        assertIndexNotExists("t1_ind_1");
    }

    @Test
    void batchIsSplitByOtherStatements() {
        AsyncSqlCursor<InternalSqlRow> cursor = gatewayNode.executeQuery(
                "INSERT INTO blackhole SELECT x FROM system_range(1, 10);"
                        + "CREATE TABLE t1 (id INT PRIMARY KEY, val_1 INT, val_2 INT);"
                        + "CREATE INDEX t1_ind_1 ON t1 (val_1);"
                        + "CREATE INDEX t1_ind_2 ON t1 (val_2);"
                        + "INSERT INTO blackhole SELECT x FROM system_range(1, 10);"
                        + "CREATE TABLE t2 (id INT PRIMARY KEY, val_1 INT, val_2 INT);"
                        + "CREATE INDEX t2_ind_1 ON t2 (val_1);"
                        + "CREATE INDEX t2_ind_2 ON t2 (val_2);"
        );

        // INSERT INTO blackhole SELECT x FROM system_range(1, 10)
        assertThat(cursor.closeAsync(), willSucceedFast());
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willSucceedFast());

        // CREATE TABLE t1 (id INT PRIMARY KEY, val_1 INT, val_2 INT)
        cursor = cursor.nextResult().join();
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willSucceedFast());

        // CREATE INDEX t1_ind_1 ON t1 (val_1)
        cursor = cursor.nextResult().join();
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willSucceedFast());

        // CREATE INDEX t1_ind_2 ON t1 (val_2)
        cursor = cursor.nextResult().join();
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willSucceedFast());

        // INSERT INTO blackhole SELECT x FROM system_range(1, 10)
        cursor = cursor.nextResult().join();
        assertThat(cursor.closeAsync(), willSucceedFast());
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willSucceedFast());

        // CREATE TABLE t2 (id INT PRIMARY KEY, val_1 INT, val_2 INT)
        cursor = cursor.nextResult().join();
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willSucceedFast());

        // CREATE INDEX t2_ind_1 ON t2 (val_1)
        cursor = cursor.nextResult().join();
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willSucceedFast());

        // CREATE INDEX t2_ind_2 ON t2 (val_2)
        cursor = cursor.nextResult().join();
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(false));

        assertEquals(2, executeCallCounter.get());

        assertTableExists("t1");
        assertIndexExists("t1_ind_1");
        assertIndexExists("t1_ind_2");

        assertTableExists("t2");
        assertIndexExists("t2_ind_1");
        assertIndexExists("t2_ind_2");
    }

    @Test
    void executionTimeErrorDuringDdlBatchProcessing() {
        AsyncSqlCursor<InternalSqlRow> cursor = gatewayNode.executeQuery(
                "CREATE TABLE t1 (id INT PRIMARY KEY, val_1 INT, val_2 INT);"
                        + "CREATE INDEX t1_ind_1 ON t1 (val_1);"
                        // next statement should fail because table with the same name exists
                        + "CREATE TABLE t1 (id INT PRIMARY KEY, val_1 INT, val_2 INT);"
                        + "CREATE INDEX t2_ind_1 ON t2 (val_1);"
        );

        // CREATE TABLE t1 (id INT PRIMARY KEY, val_1 INT, val_2 INT)
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willSucceedFast());

        // CREATE INDEX t1_ind_1 ON t1 (val_1)
        cursor = cursor.nextResult().join();
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willThrowFast(
                SqlException.class,
                "Table with name 'PUBLIC.T1' already exists"
        ));

        assertEquals(
                1 /* attempt to execute an entire batch at once */
                        + 1 /* creation of table T1 */
                        + 2 /* creation and activation of index T1_IND_1 */
                        + 1 /* attempt to create table T1 once again */,
                executeCallCounter.get()
        );

        assertTableExists("t1");
        assertIndexExists("t1_ind_1");

        assertTableNotExists("t2");
        assertIndexNotExists("t2_ind_1");
    }

    @Test
    void preparationTimeErrorInTheMiddleOfBatch() {
        AsyncSqlCursor<InternalSqlRow> cursor = gatewayNode.executeQuery(
                "CREATE TABLE t1 (id INT PRIMARY KEY, val_1 INT, val_2 INT);"
                        + "CREATE INDEX t1_ind_1 ON t1 (val_1);"
                        // next statement should fail because table without PK is not allowed
                        + "CREATE TABLE t2 (id INT, val_1 INT, val_2 INT);"
                        + "CREATE INDEX t2_ind_1 ON t2 (val_1);"
        );

        // CREATE TABLE t1 (id INT PRIMARY KEY, val_1 INT, val_2 INT)
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willSucceedFast());

        // CREATE INDEX t1_ind_1 ON t1 (val_1)
        cursor = cursor.nextResult().join();
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willThrowFast(
                SqlException.class,
                "Table without PRIMARY KEY is not supported"
        ));

        assertEquals(
                1,
                executeCallCounter.get()
        );

        assertTableExists("t1");
        assertIndexExists("t1_ind_1");

        assertTableNotExists("t2");
        assertIndexNotExists("t2_ind_1");
    }

    @Test
    void preparationTimeErrorAsFirstItemOfBatch() {
        AsyncSqlCursor<InternalSqlRow> cursor = gatewayNode.executeQuery(
                "INSERT INTO blackhole SELECT x FROM system_range(1, 10);"
                        // next statement should fail because table without PK is not allowed
                        + "CREATE TABLE t1 (id INT, val_1 INT, val_2 INT);"
                        + "CREATE INDEX t1_ind_1 ON t2 (val_1);"
        );

        // INSERT INTO blackhole SELECT x FROM system_range(1, 10)
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willThrowFast(
                SqlException.class,
                "Table without PRIMARY KEY is not supported"
        ));

        assertEquals(
                0,
                executeCallCounter.get()
        );

        assertTableNotExists("t1");
        assertIndexNotExists("t1_ind_1");
    }

    /**
     * This case makes sure that exception thrown is matched the order of execution, and not the order
     * exceptions appear.
     *
     * <p>To be more specific, first seen exception relates to absent PK definition in 3rd statement, but
     * during execution the exception that should be thrown is the one denoting that table with given name
     * already exists (2nd statement).
     */
    @Test
    void mixedErrorsInBatch() {
        AsyncSqlCursor<InternalSqlRow> cursor = gatewayNode.executeQuery(
                "CREATE TABLE t1 (id INT PRIMARY KEY, val_1 INT, val_2 INT);"
                        + "CREATE TABLE t1 (id INT PRIMARY KEY, val_1 INT, val_2 INT);"
                        + "CREATE TABLE t2 (id INT, val_1 INT, val_2 INT);"
                        + "CREATE INDEX t1_ind_1 ON t1 (val_1);"
        );

        // CREATE TABLE t1 (id INT PRIMARY KEY, val_1 INT, val_2 INT)
        assertDdlResult(cursor, true);
        assertThat(cursor.hasNextResult(), is(true));
        assertThat(cursor.nextResult(), willThrowFast(
                SqlException.class,
                "Table with name 'PUBLIC.T1' already exists"
        ));

        assertEquals(
                1 /* attempt to execute an entire batch at once */
                        + 1 /* creation of table T1 */
                        + 1 /* attempt to create table T1 once again */,
                executeCallCounter.get()
        );

        assertTableExists("t1");
        assertTableNotExists("t2");
        assertIndexNotExists("t1_ind_1");
    }

    private static void assertDdlResult(AsyncSqlCursor<InternalSqlRow> cursor, boolean expectedApplied) {
        CompletableFuture<BatchedResult<InternalSqlRow>> resultFuture = cursor.requestNextAsync(2);

        assertThat(resultFuture, willSucceedFast());

        BatchedResult<InternalSqlRow> batch = resultFuture.join();

        assertThat(batch.hasMore(), is(false));
        assertThat(batch.items(), hasSize(1));
        assertThat(batch.items().get(0).get(0), is(expectedApplied));
    }

    private void assertSchemaExists(String name) {
        CatalogService catalogService = cluster.catalogManager();

        int latestVersion = catalogService.latestCatalogVersion();
        Catalog catalog = catalogService.catalog(latestVersion);

        assertThat(catalog, notNullValue());
        assertThat(catalog.schema(IgniteNameUtils.parseIdentifier(name)), notNullValue());
    }

    private void assertTableExists(QualifiedName name) {
        CatalogService catalogService = cluster.catalogManager();

        int latestVersion = catalogService.latestCatalogVersion();
        Catalog catalog = catalogService.catalog(latestVersion);

        assertThat(catalog, notNullValue());
        assertThat(catalog.table(name.schemaName(), name.objectName()), notNullValue());
    }

    private void assertTableExists(String name) {
        assertTableExists(QualifiedName.fromSimple(name));
    }

    @SuppressWarnings("SameParameterValue")
    private void assertTableNotExists(String name) {
        CatalogService catalogService = cluster.catalogManager();

        int latestVersion = catalogService.latestCatalogVersion();
        Catalog catalog = catalogService.catalog(latestVersion);

        QualifiedName qualifiedName = QualifiedName.fromSimple(name);

        assertThat(catalog, notNullValue());
        assertThat(catalog.table(qualifiedName.schemaName(), qualifiedName.objectName()), nullValue());
    }

    private void assertIndexExists(String name) {
        assertIndexExists(QualifiedName.fromSimple(name));
    }

    private void assertIndexExists(QualifiedName name) {
        CatalogService catalogService = cluster.catalogManager();

        int latestVersion = catalogService.latestCatalogVersion();
        Catalog catalog = catalogService.catalog(latestVersion);

        assertThat(catalog, notNullValue());
        assertThat(catalog.aliveIndex(name.schemaName(), name.objectName()), notNullValue());
    }

    @SuppressWarnings("SameParameterValue")
    private void assertIndexNotExists(String name) {
        CatalogService catalogService = cluster.catalogManager();

        int latestVersion = catalogService.latestCatalogVersion();
        Catalog catalog = catalogService.catalog(latestVersion);

        QualifiedName qualifiedName = QualifiedName.fromSimple(name);

        assertThat(catalog, notNullValue());
        assertThat(catalog.aliveIndex(qualifiedName.schemaName(), qualifiedName.objectName()), nullValue());
    }

    private CatalogManager catalogManagerDecorator(CatalogManager original) {
        return (CatalogManager) Proxy.newProxyInstance(
                QueryTimeoutTest.class.getClassLoader(),
                new Class<?>[] {CatalogManager.class},
                (proxy, method, args) -> {
                    if ("execute".equals(method.getName())) {
                        executeCallCounter.incrementAndGet();
                    }

                    return method.invoke(original, args);
                }
        );
    }
}
