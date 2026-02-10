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

package org.apache.ignite.internal.sql.engine;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.CursorUtils;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.lang.CancellationToken;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base class for SQL multi-statement queries integration tests.
 */
public abstract class BaseSqlMultiStatementTest extends BaseSqlIntegrationTest {
    private final List<AsyncSqlCursor<?>> cursorsToClose = new ArrayList<>();

    @BeforeEach
    void cleanupResources() {
        cursorsToClose.forEach(cursor -> await(cursor.closeAsync()));
        cursorsToClose.clear();
    }

    @AfterEach
    protected void checkNoPendingTransactionsAndOpenedCursors() {
        waitUntilActiveTransactionsCount(is(0));

        Awaitility.await().timeout(5, TimeUnit.SECONDS).untilAsserted(
                () -> assertThat(queryProcessor().openedCursors(), is(0))
        );
    }

    /** Fully executes multi-statements query without reading cursor data. */
    void executeScript(String query, Object ... params) {
        executeScript(query, null, params);
    }

    /** Fully executes multi-statements query without reading cursor data. */
    void executeScript(String query, @Nullable InternalTransaction tx, Object ... params) {
        iterateThroughResultsAndCloseThem(runScript(tx, null, query, params));
    }

    /** Initiates multi-statements query execution. */
    protected AsyncSqlCursor<InternalSqlRow> runScript(String query, Object... params) {
        return runScript(null, null, query, params);
    }

    /** Initiates multi-statements query execution. */
    AsyncSqlCursor<InternalSqlRow> runScript(CancellationToken cancellationToken, String query, Object... params) {
        return runScript(null, cancellationToken, query, params);
    }

    AsyncSqlCursor<InternalSqlRow> runScript(@Nullable InternalTransaction tx, @Nullable CancellationToken cancellationToken, String query,
            Object... params) {
        SqlProperties properties = new SqlProperties()
                .allowedQueryTypes(SqlQueryType.ALL)
                .allowMultiStatement(true);

        AsyncSqlCursor<InternalSqlRow> cursor = await(
                queryProcessor().queryAsync(properties, observableTimeTracker(), tx, cancellationToken, query, params)
        );

        return Objects.requireNonNull(cursor);
    }

    protected List<AsyncSqlCursor<InternalSqlRow>> fetchAllCursors(AsyncSqlCursor<InternalSqlRow> cursor) {
        return fetchCursors(cursor, -1, false);
    }

    protected List<AsyncSqlCursor<InternalSqlRow>> fetchCursors(AsyncSqlCursor<InternalSqlRow> cursor, int count, boolean close) {
        List<AsyncSqlCursor<InternalSqlRow>> cursors = new ArrayList<>();

        cursors.add(cursor);

        if (!close) {
            cursorsToClose.add(cursor);
        }

        while ((count < 0 || --count > 0) && cursor.hasNextResult()) {
            cursor = await(cursor.nextResult());

            assertNotNull(cursor);

            cursors.add(cursor);

            if (!close) {
                cursorsToClose.add(cursor);
            }
        }

        if (close) {
            List<CompletableFuture<?>> closeCursorFutures = new ArrayList<>(cursors.size());
            cursors.forEach(c -> closeCursorFutures.add(c.closeAsync()));
            await(CompletableFutures.allOf(closeCursorFutures));
        }

        return cursors;
    }

    static void iterateThroughResultsAndCloseThem(AsyncSqlCursor<InternalSqlRow> cursor) {
        Function<AsyncSqlCursor<InternalSqlRow>, CompletableFuture<AsyncSqlCursor<InternalSqlRow>>> traverser = new Function<>() {
            @Override
            public CompletableFuture<AsyncSqlCursor<InternalSqlRow>> apply(AsyncSqlCursor<InternalSqlRow> cur) {
                return cur.closeAsync()
                        .thenCompose(none -> {
                            if (cur.hasNextResult()) {
                                return cur.nextResult().thenCompose(this);
                            } else {
                                return completedFuture(cur);
                            }
                        });
            }
        };

        await(completedFuture(cursor).thenCompose(traverser));
    }

    static void validateSingleResult(AsyncSqlCursor<InternalSqlRow> cursor, Object... expected) {
        BatchedResult<InternalSqlRow> res = await(cursor.requestNextAsync(1));
        assertNotNull(res);

        Function<InternalSqlRow, List<Object>> rowConverter = internalRow -> {
            List<Object> row = new ArrayList<>(internalRow.fieldCount());
            for (int i = 0; i < internalRow.fieldCount(); i++) {
                row.add(internalRow.get(i));
            }

            return row;
        };

        if (expected.length == 0) {
            assertThat(res.items(), empty());
        } else {
            List<InternalSqlRow> items = res.items();
            List<Object> rows = items.stream()
                    .map(rowConverter)
                    .collect(Collectors.toList());

            assertEquals(List.of(List.of(expected)), rows);
        }

        if (res.hasMore()) {
            List<Object> remainingRows = CursorUtils.getAllFromCursor(cursor)
                    .stream()
                    .map(rowConverter)
                    .collect(Collectors.toList());

            fail("Cursor must not have more data, but was: " + remainingRows);
        }

        cursor.closeAsync();
    }
}
