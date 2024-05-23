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

package org.apache.ignite.internal.sql.threading;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.PublicApiThreadingTests.anIgniteThread;
import static org.apache.ignite.internal.PublicApiThreadingTests.asyncContinuationPool;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.is;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.PublicApiThreadingTests;
import org.apache.ignite.internal.sql.api.IgniteSqlImpl;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@SuppressWarnings("resource")
class ItSqlApiThreadingTest extends ClusterPerClassIntegrationTest {
    private static final String TABLE_NAME = "test";

    private static final String SELECT_QUERY = "SELECT * FROM " + TABLE_NAME;
    private static final String UPDATE_QUERY = "UPDATE " + TABLE_NAME + " SET val = val WHERE id = ?";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeAll
    void createTable() {
        sql("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, val VARCHAR)");

        plainKeyValueView().putAll(null, Map.of(1, "1", 2, "2", 3, "3"));
    }

    private static KeyValueView<Integer, String> plainKeyValueView() {
        return testTable().keyValueView(Integer.class, String.class);
    }

    private static Table testTable() {
        return CLUSTER.aliveNode().tables().table(TABLE_NAME);
    }

    private static IgniteSql igniteSqlForInternalUse() {
        return Wrappers.unwrap(igniteSqlForPublicUse(), IgniteSqlImpl.class);
    }

    @ParameterizedTest
    @EnumSource(SqlAsyncOperation.class)
    void sqlFuturesCompleteInContinuationsPool(SqlAsyncOperation operation) {
        CompletableFuture<Thread> completerFuture = forcingSwitchFromUserThread(
                () -> operation.executeOn(igniteSqlForPublicUse())
                        .thenApply(unused -> currentThread())
        );

        assertThat(completerFuture, willBe(asyncContinuationPool()));
    }

    private static IgniteSql igniteSqlForPublicUse() {
        return CLUSTER.aliveNode().sql();
    }

    private static <T> T forcingSwitchFromUserThread(Supplier<? extends T> action) {
        return PublicApiThreadingTests.tryToSwitchFromUserThreadWithDelayedSchemaSync(CLUSTER.aliveNode(), action);
    }

    @ParameterizedTest
    @EnumSource(SqlAsyncOperation.class)
    void sqlFuturesFromInternalCallsAreNotResubmittedToContinuationsPool(SqlAsyncOperation operation) {
        CompletableFuture<Thread> completerFuture = forcingSwitchFromUserThread(
                () -> operation.executeOn(igniteSqlForInternalUse())
                        .thenApply(unused -> currentThread())
        );

        assertThat(completerFuture, willBe(anIgniteThread()));
    }

    @ParameterizedTest
    @EnumSource(AsyncResultSetAsyncOperation.class)
    void asyncResultSetFuturesCompleteInContinuationsPool(AsyncResultSetAsyncOperation operation) throws Exception {
        AsyncResultSet<?> firstPage = fetchFirstPage(igniteSqlForPublicUse());

        CompletableFuture<Thread> completerFuture = operation.executeOn(firstPage)
                        .thenApply(unused -> currentThread());

        // The future might get completed in the calling thread as we don't force a wait inside Ignite
        // (because we cannot do this with fetching next page or closing).
        assertThat(completerFuture, willBe(
                either(is(currentThread())).or(asyncContinuationPool())
        ));
    }

    private static AsyncResultSet<SqlRow> fetchFirstPage(IgniteSql igniteSql)
            throws InterruptedException, ExecutionException, TimeoutException {
        // Setting a minimum page size to ensure that the query engine returns a non-closed
        // cursor even after we call its second page.
        Statement statement = igniteSql.statementBuilder().query(SELECT_QUERY).pageSize(1).build();

        return igniteSql.executeAsync(null, statement).get(10, SECONDS);
    }

    @ParameterizedTest
    @EnumSource(AsyncResultSetAsyncOperation.class)
    void asyncResultSetFuturesFromInternalCallsAreNotResubmittedToContinuationsPool(AsyncResultSetAsyncOperation operation)
            throws Exception {
        AsyncResultSet<?> firstPage = fetchFirstPage(igniteSqlForInternalUse());

        CompletableFuture<Thread> completerFuture = operation.executeOn(firstPage)
                .thenApply(unused -> currentThread());

        // The future might get completed in the calling thread as we don't force a wait inside Ignite
        // (because we cannot do this with fetching next page or closing).
        assertThat(completerFuture, willBe(
                either(is(currentThread())).or(anIgniteThread())
        ));
    }

    /**
     * This test differs from {@link #asyncResultSetFuturesCompleteInContinuationsPool(AsyncResultSetAsyncOperation)} in that it obtains
     * the future to test from a call on a ResultSet obtained from a ResultSet, not from a view.
     */
    @ParameterizedTest
    @EnumSource(AsyncResultSetAsyncOperation.class)
    void asyncResultSetFuturesAfterFetchCompleteInContinuationsPool(AsyncResultSetAsyncOperation operation) throws Exception {
        AsyncResultSet<?> firstPage = fetchFirstPage(igniteSqlForPublicUse());
        AsyncResultSet<?> secondPage = firstPage.fetchNextPage().get(10, SECONDS);

        CompletableFuture<Thread> completerFuture = operation.executeOn(secondPage)
                .thenApply(unused -> currentThread());

        // The future might get completed in the calling thread as we don't force a wait inside Ignite
        // (because we cannot do this with fetching next page or closing).
        assertThat(completerFuture, willBe(
                either(is(currentThread())).or(asyncContinuationPool())
        ));
    }

    /**
     * This test differs from
     * {@link #asyncResultSetFuturesFromInternalCallsAreNotResubmittedToContinuationsPool(AsyncResultSetAsyncOperation)} in that
     * it obtains the future to test from a call on a ResultSet obtained from a ResultSet, not from a view.
     */
    @ParameterizedTest
    @EnumSource(AsyncResultSetAsyncOperation.class)
    void asyncResultSetFuturesAfterFetchFromInternalCallsAreNotResubmittedToContinuationsPool(AsyncResultSetAsyncOperation operation)
            throws Exception {
        AsyncResultSet<?> firstPage = fetchFirstPage(igniteSqlForInternalUse());
        AsyncResultSet<?> secondPage = firstPage.fetchNextPage().get(10, SECONDS);

        CompletableFuture<Thread> completerFuture = operation.executeOn(secondPage)
                .thenApply(unused -> currentThread());

        // The future might get completed in the calling thread as we don't force a wait inside Ignite
        // (because we cannot do this with fetching next page or closing).
        assertThat(completerFuture, willBe(
                either(is(currentThread())).or(anIgniteThread())
        ));
    }

    private enum SqlAsyncOperation {
        EXECUTE_QUERY_ASYNC(sql -> sql.executeAsync(null, SELECT_QUERY)),
        EXECUTE_STATEMENT_ASYNC(sql -> sql.executeAsync(null, sql.createStatement(SELECT_QUERY))),
        // TODO: IGNITE-18695 - uncomment 2 following lines.
        // EXECUTE_QUERY_WITH_MAPPER_ASYNC(sql -> sql.executeAsync(null, (Mapper<?>) null, SELECT_QUERY)),
        // EXECUTE_STATEMENT_WITH_MAPPER_ASYNC(sql -> sql.executeAsync(null, (Mapper<?>) null, sql.createStatement(SELECT_QUERY))),
        EXECUTE_BATCH_QUERY_ASYNC(sql -> sql.executeBatchAsync(null, UPDATE_QUERY, BatchedArguments.of(10_000))),
        EXECUTE_BATCH_STATEMENT_ASYNC(sql -> sql.executeBatchAsync(null, sql.createStatement(UPDATE_QUERY), BatchedArguments.of(10_000))),
        EXECUTE_SCRIPT_ASYNC(sql -> sql.executeScriptAsync(SELECT_QUERY));

        private final Function<IgniteSql, CompletableFuture<?>> action;

        SqlAsyncOperation(Function<IgniteSql, CompletableFuture<?>> action) {
            this.action = action;
        }

        CompletableFuture<?> executeOn(IgniteSql sql) {
            return action.apply(sql);
        }
    }

    private enum AsyncResultSetAsyncOperation {
        FETCH_NEXT_PAGE(resultSet -> resultSet.fetchNextPage()),
        CLOSE(resultSet -> resultSet.closeAsync());

        private final Function<AsyncResultSet<Object>, CompletableFuture<?>> action;

        AsyncResultSetAsyncOperation(Function<AsyncResultSet<Object>, CompletableFuture<?>> action) {
            this.action = action;
        }

        CompletableFuture<?> executeOn(AsyncResultSet<?> resultSet) {
            return action.apply((AsyncResultSet<Object>) resultSet);
        }
    }
}
