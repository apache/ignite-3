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

package org.apache.ignite.internal.runner.app.client;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.expectQueryCancelled;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.lang.ErrorGroups.Sql.RUNTIME_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.TX_CONTROL_INSIDE_EXTERNAL_TX_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ALREADY_FINISHED_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.internal.client.sql.ClientAsyncResultSet;
import org.apache.ignite.internal.client.sql.ClientSql;
import org.apache.ignite.internal.client.sql.QueryModifier;
import org.apache.ignite.internal.sql.SyncResultSetAdapter;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.lang.CancelHandle;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Thin client SQL multi-statement integration test.
 *
 * <p>Tests to check internal API for reading script execution results.
 */
@SuppressWarnings("resource")
public class ItThinClientMultistatementSqlTest extends ItAbstractThinClientTest {
    private final List<ClientAsyncResultSet<SqlRow>> resultsToClose = new ArrayList<>();

    private int resourcesBefore;

    @BeforeEach
    void setup() {
        resultsToClose.forEach(resultSet -> await(resultSet.closeAsync()));

        resourcesBefore = countResources();
    }

    @AfterEach
    protected void checkNoPendingTransactionsAndOpenedCursors() {
        Awaitility.await().timeout(5, TimeUnit.SECONDS).untilAsserted(() -> {
            for (int i = 0; i < nodes(); i++) {
                assertThat("node=" + i, queryProcessor(i).openedCursors(), is(0));
            }

            for (int i = 0; i < nodes(); i++) {
                assertThat("node=" + i, txManager(i).pending(), is(0));
            }

            assertThat(countResources() - resourcesBefore, is(0));

            for (int i = 0; i < nodes(); i++) {
                int cancelHandlesCount = unwrapIgniteImpl(server(i)).clientInboundMessageHandler().cancelHandlesCount();

                assertThat("node=" + i, cancelHandlesCount, is(0));
            }
        });

        String dropTablesScript = client().tables().tables().stream()
                .map(Table::name)
                .map(name -> "DROP TABLE " + name)
                .collect(Collectors.joining(";\n"));

        if (!dropTablesScript.isEmpty()) {
            client().sql().executeScript(dropTablesScript);
        }
    }

    /** Ensures that we can get the next result set after the current one is closed. */
    @Test
    void checkIterationOverResultSets() {
        ClientAsyncResultSet<SqlRow> asyncRs = runSql("SELECT 1; SELECT 2; SELECT 3;");

        // First result set.
        {
            SyncResultSetAdapter<SqlRow> rs = new SyncResultSetAdapter<>(asyncRs);
            assertThat(rs.hasRowSet(), is(true));
            assertThat(rs.next().intValue(0), is(1));

            rs.close();
        }

        assertThat(asyncRs.hasNextResultSet(), is(true));

        CompletableFuture<ClientAsyncResultSet<SqlRow>> nextResultFut = asyncRs.nextResultSet();
        // Ensures that the next result is cached locally
        // and subsequent calls do not request data from the server.
        assertThat(nextResultFut, is(asyncRs.nextResultSet()));

        asyncRs = await(asyncRs.nextResultSet());

        // Second result set.
        {
            SyncResultSetAdapter<SqlRow> rs = new SyncResultSetAdapter<>(asyncRs);
            assertThat(rs.hasRowSet(), is(true));
            assertThat(rs.next().intValue(0), is(2));

            rs.close();
        }

        assertThat(asyncRs.hasNextResultSet(), is(true));
        // Ensures that the next result is cached locally
        // and subsequent calls do not request data from the server.
        assertThat(asyncRs.nextResultSet(), is(asyncRs.nextResultSet()));
        asyncRs = await(asyncRs.nextResultSet());

        // Second result set.
        {
            SyncResultSetAdapter<SqlRow> rs = new SyncResultSetAdapter<>(asyncRs);
            assertThat(rs.hasRowSet(), is(true));
            assertThat(rs.next().intValue(0), is(3));

            rs.close();
        }

        assertThat(asyncRs.hasNextResultSet(), is(false));
    }

    @Test
    void basicMultiStatementQuery() {
        String sql = "CREATE TABLE test (id INT PRIMARY KEY, val INT);"
                + "INSERT INTO test VALUES (3, 3);"
                + "UPDATE test SET val=7 WHERE id=3;"
                + "EXPLAIN PLAN FOR SELECT * FROM test;"
                + "SELECT * FROM test;"
                + "DELETE FROM test;"
                + "DROP TABLE IF EXISTS non_existing_table;";

        List<ClientAsyncResultSet<SqlRow>> resultSets = fetchAllResults(runSql(sql));
        Iterator<ClientAsyncResultSet<SqlRow>> curItr = resultSets.iterator();

        ResultValidator.ddl(curItr.next(), true);
        ResultValidator.dml(curItr.next(), 1);
        ResultValidator.dml(curItr.next(), 1);
        assertNotNull(curItr.next()); // skip EXPLAIN.
        ResultValidator.singleRow(curItr.next(), 3, 7);
        ResultValidator.dml(curItr.next(), 1);
        ResultValidator.ddl(curItr.next(), false);

        assertThat(curItr.hasNext(), is(false));

        resultSets.forEach(AsyncResultSet::closeAsync);

        // Ensures that the script is executed completely, even if the cursor data has not been read.
        executeSql("INSERT INTO test VALUES (1, 1);"
                + "INSERT INTO test VALUES (2, 2);"
                + "SELECT * FROM test;"
                + "INSERT INTO test VALUES (3, 3);");

        expectRowsCount(null, "test", 3);
    }

    @Test
    public void txControlStatement() {
        String query = "START TRANSACTION; COMMIT";

        // Execution of the TX_CONTROL statement is allowed.
        {
            EnumSet<QueryModifier> modifiers = EnumSet.of(
                    QueryModifier.ALLOW_TX_CONTROL,
                    QueryModifier.ALLOW_MULTISTATEMENT
            );

            ClientAsyncResultSet<SqlRow> startFuture = runSql((Transaction) null, null, modifiers, query);
            List<ClientAsyncResultSet<SqlRow>> resultSets = fetchAllResults(startFuture);

            assertThat(resultSets, hasSize(2));

            ClientAsyncResultSet<SqlRow> rs = resultSets.get(0);
            assertThat(rs.hasNextResultSet(), is(true));
            assertThat(rs.hasRowSet(), is(false));
            assertThat(rs.wasApplied(), is(false));
            assertThat(rs.affectedRows(), is(-1L));

            rs = resultSets.get(1);
            assertThat(rs.hasNextResultSet(), is(false));
            assertThat(rs.hasRowSet(), is(false));
            assertThat(rs.wasApplied(), is(false));
            assertThat(rs.affectedRows(), is(-1L));
        }

        // Execution of the TX_CONTROL statement is not allowed.
        {
            EnumSet<QueryModifier> modifiers = EnumSet.of(
                    QueryModifier.ALLOW_ROW_SET_RESULT,
                    QueryModifier.ALLOW_MULTISTATEMENT
            );

            assertThrowsSqlException(
                    STMT_VALIDATION_ERR,
                    "Statement of type \"Transaction control statement\" is not allowed in current context",
                    () -> runSql((Transaction) null, null, modifiers, query)
            );
        }
    }

    @Test
    void throwsNoSuchElementExceptionIfNoMoreResults() {
        ClientAsyncResultSet<SqlRow> resulSet = runSql("SELECT 1");

        assertThat(resulSet.hasNextResultSet(), is(false));

        //noinspection ThrowableNotThrown
        assertThrows(NoSuchElementException.class, () -> await(resulSet.nextResultSet()), "Query has no more results");
    }

    @Test
    void queryWithDynamicParameters() {
        String sql = "CREATE TABLE test (id INT PRIMARY KEY, val VARCHAR DEFAULT '3');"
                + "INSERT INTO test VALUES(?, ?);"
                + "INSERT INTO test VALUES(?, DEFAULT);"
                + "INSERT INTO test VALUES(?, ?);";

        executeSql(sql, 0, "1", 2, 4, "5");
        expectRowsCount(null, "test", 3);
    }

    @Test
    void explicitTransaction() {
        executeSql("CREATE TABLE test (id INT PRIMARY KEY);");

        Transaction tx = client().transactions().begin();
        executeSql(tx, "INSERT INTO test VALUES (0); INSERT INTO test VALUES (1); INSERT INTO test VALUES (2)");

        expectRowsCount(tx, "test", 3);
        expectRowsCount(null, "test", 0);

        tx.commit();

        expectRowsCount(null, "test", 3);
    }

    @Test
    void queryWithIncorrectNumberOfDynamicParametersFailsWithValidationError() {
        String sql = "CREATE TABLE test (id INT PRIMARY KEY, val INT);"
                + "INSERT INTO test VALUES(?, ?);"
                + "INSERT INTO test VALUES(?, ?);";

        String expectedMessage = "Unexpected number of query parameters";

        assertThrowsSqlException(STMT_VALIDATION_ERR, expectedMessage, () -> runSql(sql, 0));
        assertThrowsSqlException(STMT_VALIDATION_ERR, expectedMessage, () -> runSql(sql, 0, 1, 2, 3, 4, 5));
    }

    @Test
    void scriptStopsExecutionOnError() {
        // Runtime error.
        {
            assertThrowsSqlException(
                    RUNTIME_ERR,
                    "Division by zero",
                    () -> executeSql(
                            "CREATE TABLE test (id INT PRIMARY KEY);"
                                    + "INSERT INTO test VALUES (2/0);" // Runtime error.
                                    + "INSERT INTO test VALUES (0)"
                    )
            );

            expectRowsCount(null, "test", 0);
        }

        // Validation error.
        {
            assertThrowsSqlException(
                    STMT_VALIDATION_ERR,
                    "Values passed to VALUES operator must have compatible types",
                    () -> executeSql(
                            "INSERT INTO test VALUES (?), (?)",
                            "1", 2
                    )
            );

            expectRowsCount(null, "test", 0);
        }

        // Internal error.
        {
            assertThrowsSqlException(
                    RUNTIME_ERR,
                    "Subquery returned more than 1 value",
                    () -> executeSql(
                            "INSERT INTO test VALUES(0);"
                                    + "INSERT INTO test VALUES(1);"
                                    + "DELETE FROM test WHERE id = (SELECT id FROM test);" // Internal error.
                                    + "INSERT INTO test VALUES(2);"
                    )
            );

            expectRowsCount(null, "test", 2);
        }

        // Same as above, but inside script managed transaction.
        {
            assertThrowsSqlException(
                    RUNTIME_ERR,
                    "Subquery returned more than 1 value",
                    () -> executeSql(
                            "START TRANSACTION;"
                                    + "INSERT INTO test VALUES(2);"
                                    + "INSERT INTO test VALUES(3);"
                                    + "DELETE FROM test WHERE id = (SELECT id FROM test);" // Internal error.
                                    + "INSERT INTO test VALUES(4);"
                                    + "COMMIT;"
                    )
            );

            expectRowsCount(null, "test", 2);
        }

        // Attempt to start script-managed transaction inside external transaction.
        {
            Transaction tx = client().transactions().begin();

            assertThrowsSqlException(
                    TX_CONTROL_INSIDE_EXTERNAL_TX_ERR,
                    "Transaction control statement cannot be executed within an external transaction.",
                    () -> executeSql(
                            tx,
                            "START TRANSACTION; COMMIT;"
                    )
            );

            tx.rollback();
        }

        // Internal error due to transaction exception.
        {
            Transaction tx = client().transactions().begin();
            client().sql().execute(tx, "INSERT INTO test VALUES(2);").close();
            tx.commit();

            assertThrowsSqlException(
                    TX_ALREADY_FINISHED_ERR,
                    "Transaction is already finished",
                    () -> executeSql(
                            tx,
                            "INSERT INTO test VALUES(3); INSERT INTO test VALUES(4);"
                    )
            );

            expectRowsCount(null, "test", 3);
        }
    }

    @Test
    public void cancelScript() {
        StringBuilder query = new StringBuilder();

        int statementsCount = 100;

        for (int j = 0; j < statementsCount; j++) {
            query.append("SELECT x FROM TABLE(SYSTEM_RANGE(0, 100))").append(";");
        }

        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        List<ClientAsyncResultSet<SqlRow>> allResults = fetchAllResults(runSql((Transaction) null, token, null, query.toString()));

        assertThat(allResults, hasSize(statementsCount));

        cancelHandle.cancel();

        allResults.forEach(rs -> {
            expectQueryCancelled(() -> {
                AsyncResultSet<SqlRow> res;
                do {
                    res = await(rs.fetchNextPage());
                } while (res.hasMorePages());
            });

            rs.closeAsync();
        });
    }

    @Test
    public void executeScriptWithErrors() {
        client().sql().executeScript("SELECT 1; SELECT 2/0; SELECT 3;");
    }

    @Test
    public void iterateOverScriptWithErrors() {
        ClientAsyncResultSet<SqlRow> rs = runSql("SELECT 1; SELECT 2/0; SELECT 3; SELECT 4;");
        try {
            rs.nextResultSet().join();
        } catch (Exception ignore) {
            // Resources should not leak after this error.
        }
    }

    private void expectRowsCount(@Nullable Transaction tx, String table, long expectedCount) {
        try (ResultSet<SqlRow> rs = client().sql().execute(tx, "SELECT COUNT(*) FROM " + table)) {
            assertThat(rs.next().longValue(0), is(expectedCount));
        }
    }

    private SqlQueryProcessor queryProcessor(int idx) {
        return ((SqlQueryProcessor) unwrapIgniteImpl(server(idx)).queryEngine());
    }

    private TxManager txManager(int idx) {
        return unwrapIgniteImpl(server(idx)).txManager();
    }

    private int countResources() {
        int count = 0;

        for (int i = 0; i < nodes(); i++) {
            ClientResourceRegistry resources = unwrapIgniteImpl(server(i)).clientInboundMessageHandler().resources();

            count += resources.size();
        }

        return count;
    }

    private ClientAsyncResultSet<SqlRow> runSql(String query, Object... args) {
        return runSql(null, null, null, query, args);
    }

    private ClientAsyncResultSet<SqlRow> runSql(
            @Nullable Transaction tx,
            @Nullable CancellationToken cancelToken,
            @Nullable Set<QueryModifier> queryModifiers,
            String query,
            Object... args
    ) {
        ClientSql clientSql = (ClientSql) client().sql();
        Statement stmt = clientSql.statementBuilder().query(query).pageSize(1).build();

        return (ClientAsyncResultSet<SqlRow>) await(
                clientSql.executeAsyncInternal(
                        tx,
                        (Mapper<SqlRow>) null,
                        cancelToken,
                        queryModifiers == null ? QueryModifier.ALL : queryModifiers,
                        stmt,
                        args
                )
        );
    }

    private void executeSql(String sql, Object... args) {
        executeSql(null, sql, args);
    }

    private void executeSql(@Nullable Transaction tx, String sql, Object... args) {
        fetchAllResults(runSql(tx, null, null, sql, args))
                .forEach(rs -> await(rs.closeAsync()));
    }

    private List<ClientAsyncResultSet<SqlRow>> fetchAllResults(ClientAsyncResultSet<SqlRow> resultSet) {
        List<ClientAsyncResultSet<SqlRow>> resultSets = new ArrayList<>();

        resultSets.add(resultSet);
        resultsToClose.add(resultSet);

        ClientAsyncResultSet<SqlRow> resultSet0 = resultSet;

        while (resultSet0.hasNextResultSet()) {
            resultSet0 = await(resultSet0.nextResultSet());

            assertNotNull(resultSet0);

            resultSets.add(resultSet0);

            resultsToClose.add(resultSet0);
        }

        return resultSets;
    }

    private static class ResultValidator {
        static void dml(ClientAsyncResultSet<SqlRow> resultSet, long affectedRows) {
            assertThat(resultSet.hasRowSet(), is(false));
            assertThat(resultSet.affectedRows(), is(affectedRows));
        }

        private static void ddl(ClientAsyncResultSet<SqlRow> resultSet, boolean wasApplied) {
            assertThat(resultSet.hasRowSet(), is(false));
            assertThat(resultSet.wasApplied(), is(wasApplied));
        }

        private static void singleRow(ClientAsyncResultSet<SqlRow> resultSet, Object... expected) {
            assertThat(resultSet.hasRowSet(), is(true));
            Iterator<SqlRow> pageIter = resultSet.currentPage().iterator();
            SqlRow row = pageIter.next();
            int rowSize = row.metadata().columns().size();
            List<Object> actual = new ArrayList<>(rowSize);

            for (int i = 0; i < rowSize; i++) {
                actual.add(row.value(i));
            }

            assertThat(List.of(expected), equalTo(actual));
            assertThat(pageIter.hasNext(), is(false));
        }
    }
}
