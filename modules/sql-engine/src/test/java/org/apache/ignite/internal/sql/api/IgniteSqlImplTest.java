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

package org.apache.ignite.internal.sql.api;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.cause;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.util.ListToInternalSqlRowAdapter;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.tx.IgniteTransactions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests to verify {@link IgniteSqlImpl}.
 */
@SuppressWarnings("ThrowableNotThrown")
@ExtendWith(MockitoExtension.class)
class IgniteSqlImplTest extends BaseIgniteAbstractTest {
    private final AtomicLong clock = new AtomicLong();

    @Mock
    private QueryProcessor queryProcessor;

    private IgniteSqlImpl igniteSql;

    @BeforeEach
    void setUp() {
        clock.set(1L);

        igniteSql = new IgniteSqlImpl(queryProcessor, mock(IgniteTransactions.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void resultSetIsUnregisteredWhenUnderlyingCursorIsClosed() {
        AsyncSqlCursor<InternalSqlRow> result = mock(AsyncSqlCursor.class);

        when(result.requestNextAsync(anyInt()))
                .thenReturn(completedFuture(new BatchedResult<>(List.of(new ListToInternalSqlRowAdapter(List.of(0L))), true)));

        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        when(result.onClose())
                .thenReturn(closeFuture);

        when(queryProcessor.queryAsync(any(), any(), any(), any(), any(Object[].class)))
                .thenReturn(completedFuture(result));

        AsyncResultSet<?> rs = await(igniteSql.executeAsync(null, "SELECT 1"));

        assertThat(rs, notNullValue());
        assertThat(igniteSql.openedCursors(), hasSize(1));

        closeFuture.complete(null);

        assertThat(igniteSql.openedCursors(), empty());
    }

    @Test
    @SuppressWarnings("unchecked")
    void internalCursorIsClosedWhenNodeIsStopped() throws Exception {
        AsyncSqlCursor<InternalSqlRow> result = mock(AsyncSqlCursor.class);

        when(result.requestNextAsync(anyInt()))
                .thenReturn(completedFuture(new BatchedResult<>(List.of(new ListToInternalSqlRowAdapter(List.of(0L))), true)));
        when(result.onClose()).thenReturn(new CompletableFuture<>());
        when(result.closeAsync()).thenReturn(nullCompletedFuture());

        when(queryProcessor.queryAsync(any(), any(), any(), any(), any(Object[].class)))
                .thenReturn(completedFuture(result));

        AsyncResultSet<?> rs = await(igniteSql.executeAsync(null, "SELECT 1"));

        assertThat(rs, notNullValue());
        assertThat(igniteSql.openedCursors(), hasSize(1));

        igniteSql.stop();

        verify(result).closeAsync();
    }

    @Test
    @SuppressWarnings("unchecked")
    void resultSetIsNotCreatedIfComponentIsStoppedInMiddleOfOperation() throws Exception {
        CompletableFuture<AsyncSqlCursor<InternalSqlRow>> cursorFuture = new CompletableFuture<>();
        CountDownLatch executeQueryLatch = new CountDownLatch(1);
        when(queryProcessor.queryAsync(any(), any(), any(), any(), any(Object[].class)))
                .thenAnswer(ignored -> {
                    executeQueryLatch.countDown();
                    return cursorFuture;
                });

        CompletableFuture<?> result = igniteSql.executeAsync(null, "SELECT 1");

        assertThat(executeQueryLatch.await(5, TimeUnit.SECONDS), is(true));

        igniteSql.stop();

        AsyncSqlCursor<InternalSqlRow> cursor = mock(AsyncSqlCursor.class);
        cursorFuture.complete(cursor);

        assertThrowsSqlException(
                NODE_STOPPING_ERR,
                "Node is stopping",
                () -> await(result)
        );
        assertThat(igniteSql.openedCursors(), empty());
        verify(cursor).closeAsync();
    }

    @Test
    @SuppressWarnings("unchecked")
    void batchProcessingIsInterruptedIfComponentIsStoppedInMiddle() throws Exception {
        AsyncSqlCursor<InternalSqlRow> dummyResult = mock(AsyncSqlCursor.class);

        when(dummyResult.requestNextAsync(anyInt()))
                .thenReturn(completedFuture(new BatchedResult<>(List.of(new ListToInternalSqlRowAdapter(List.of(0L))), false)));
        when(dummyResult.closeAsync())
                .thenReturn(nullCompletedFuture());

        CompletableFuture<AsyncSqlCursor<InternalSqlRow>> cursorFuture = new CompletableFuture<>();
        CountDownLatch executeQueryLatch = new CountDownLatch(3);
        when(queryProcessor.queryAsync(any(), any(), any(), any(), any(Object[].class)))
                .thenAnswer(ignored -> {
                    executeQueryLatch.countDown();

                    if (executeQueryLatch.getCount() > 0) {
                        return completedFuture(dummyResult);
                    }

                    return cursorFuture;
                });

        BatchedArguments args = BatchedArguments.create();
        for (int i = 0; i < 10; i++) {
            args.add(i);
        }

        CompletableFuture<?> result = igniteSql.executeBatchAsync(null, "SELECT 1", args);

        assertThat(executeQueryLatch.await(5, TimeUnit.SECONDS), is(true));

        igniteSql.stop();

        AsyncSqlCursor<InternalSqlRow> cursor = mock(AsyncSqlCursor.class);
        cursorFuture.complete(cursor);

        assertThrowsSqlException(
                NODE_STOPPING_ERR,
                "Node is stopping",
                () -> await(result)
        );
        assertThat(igniteSql.openedCursors(), empty());
        verify(queryProcessor, times(3)).queryAsync(any(), any(), any(), any(), any(Object[].class));
        verify(cursor).closeAsync();
    }

    @Test
    @SuppressWarnings("unchecked")
    void resultSetAreClosedWhenReadTillEnd() {
        AsyncSqlCursor<InternalSqlRow> result = mock(AsyncSqlCursor.class);

        AtomicBoolean hasMore = new AtomicBoolean(true);

        when(result.requestNextAsync(anyInt()))
                .thenAnswer(ignored -> completedFuture(new BatchedResult<>(List.of(List.of(0L)), hasMore.get())));
        when(result.queryType())
                .thenReturn(SqlQueryType.QUERY);

        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        when(result.closeAsync())
                .thenAnswer(ignored -> {
                    closeFuture.complete(null);

                    return closeFuture;
                });
        when(result.onClose())
                .thenReturn(closeFuture);

        when(queryProcessor.queryAsync(any(), any(), any(), any(), any(Object[].class)))
                .thenReturn(completedFuture(result));

        AsyncResultSet<?> rs = await(igniteSql.executeAsync(null, "SELECT 1"));

        assertThat(rs, notNullValue());
        assertThat(igniteSql.openedCursors(), hasSize(1));

        await(rs.fetchNextPage());
        await(rs.fetchNextPage());

        // still opened
        assertThat(igniteSql.openedCursors(), hasSize(1));

        hasMore.set(false);
        await(rs.fetchNextPage());

        assertThat(igniteSql.openedCursors(), empty());
    }

    @Test
    public void scriptIteratesOverCursors() {
        AsyncSqlCursor<InternalSqlRow> cursor1 = mock(AsyncSqlCursor.class, "cursor1");
        AsyncSqlCursor<InternalSqlRow> cursor2 = mock(AsyncSqlCursor.class, "cursor2");

        when(cursor1.hasNextResult()).thenReturn(true);
        when(cursor1.nextResult()).thenReturn(completedFuture(cursor2));
        when(cursor1.closeAsync()).thenReturn(nullCompletedFuture());

        when(cursor2.hasNextResult()).thenReturn(false);
        when(cursor2.closeAsync()).thenReturn(nullCompletedFuture());

        when(queryProcessor.queryAsync(any(), any(), any(), any(), any(Object[].class)))
                .thenReturn(completedFuture(cursor1));

        Void rs = await(igniteSql.executeScriptAsync("SELECT 1; SELECT 2"));

        assertNull(rs);
        assertThat(igniteSql.openedCursors(), empty());
    }

    @Test
    public void scriptRethrowsExceptionFromCursor() {
        AsyncSqlCursor<InternalSqlRow> cursor1 = mock(AsyncSqlCursor.class);

        when(cursor1.hasNextResult()).thenReturn(true);
        when(cursor1.nextResult()).thenReturn(CompletableFuture.failedFuture(new RuntimeException("Broken")));
        when(cursor1.closeAsync()).thenReturn(nullCompletedFuture());

        when(queryProcessor.queryAsync(any(), any(), any(), any(), any(Object[].class)))
                .thenReturn(completedFuture(cursor1));

        assertThrowsSqlException(
                INTERNAL_ERR,
                "Broken",
                () -> await(igniteSql.executeScriptAsync("SELECT 1; SELECT 2"))
        );

        assertThat(igniteSql.openedCursors(), empty());
    }

    @Test
    public void scriptRethrowsCloseCursorException() {
        AsyncSqlCursor<InternalSqlRow> cursor1 = mock(AsyncSqlCursor.class, "cursor1");
        AsyncSqlCursor<InternalSqlRow> cursor2 = mock(AsyncSqlCursor.class, "cursor2");

        Exception cursorCloseException1 = new IllegalStateException("cursor1");
        Exception cursorCloseException2 = new IllegalStateException("cursor2");
        Exception lastCursorScriptException = new IllegalStateException("Script exception");

        when(cursor1.hasNextResult()).thenReturn(true);
        when(cursor1.nextResult()).thenReturn(completedFuture(cursor2));
        when(cursor1.closeAsync()).thenReturn(CompletableFuture.failedFuture(cursorCloseException1));

        when(cursor2.hasNextResult()).thenReturn(true);
        when(cursor2.nextResult()).thenReturn(CompletableFuture.failedFuture(lastCursorScriptException));
        when(cursor2.closeAsync()).thenReturn(CompletableFuture.failedFuture(cursorCloseException2));

        when(queryProcessor.queryAsync(any(), any(), any(), any(), any(Object[].class)))
                .thenReturn(completedFuture(cursor1));

        SqlException sqlEx = assertThrowsExactly(SqlException.class, () -> await(igniteSql.executeScriptAsync("SELECT 1; SELECT 2")));

        Exception cause = cause(sqlEx, lastCursorScriptException.getClass(), lastCursorScriptException.getMessage());
        assertNotNull(cause);

        assertEquals(2, cause.getSuppressed().length);
        assertSame(cursorCloseException1, cause.getSuppressed()[0]);
        assertSame(cursorCloseException2, cause.getSuppressed()[1]);
    }

    @Test
    public void scriptTerminatesWhenComponentIsStopped() {
        AsyncSqlCursor<InternalSqlRow> cursor1 = mock(AsyncSqlCursor.class, "cursor1");
        AsyncSqlCursor<InternalSqlRow> cursor2 = mock(AsyncSqlCursor.class, "cursor2");

        CompletableFuture<AsyncSqlCursor<InternalSqlRow>> cursor2Fut = new CompletableFuture<>();

        when(cursor1.hasNextResult()).thenReturn(true);
        when(cursor1.nextResult()).thenAnswer(ignored -> cursor2Fut);
        when(cursor1.closeAsync()).thenReturn(nullCompletedFuture());

        when(cursor2.closeAsync()).thenReturn(nullCompletedFuture());

        when(queryProcessor.queryAsync(any(), any(), any(), any(), any(Object[].class)))
                .thenReturn(completedFuture(cursor1));

        Thread thread = new Thread(() -> {
            try {
                igniteSql.stop();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                cursor2Fut.complete(cursor2);
            }
        });

        assertThrowsSqlException(
                NODE_STOPPING_ERR,
                "Node is stopping",
                () -> {
                    CompletableFuture<Void> f = igniteSql.executeScriptAsync("SELECT 1; SELECT 2");
                    thread.start();
                    await(f);
                }
        );

        assertThat(igniteSql.openedCursors(), empty());
    }
}
