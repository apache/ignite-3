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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.sql.engine.property.SqlPropertiesHelper;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;

/**
 * Base class for SQL multi-statement queries integration tests.
 */
public abstract class BaseSqlMultiStatementTest extends BaseSqlIntegrationTest {
    @AfterEach
    protected void checkNoPendingTransactionsAndOpenedCursors() {
        assertEquals(0, txManager().pending());

        try {
            boolean success = waitForCondition(() -> queryProcessor().openedCursors() == 0, 5_000);

            if (!success) {
                assertEquals(0, queryProcessor().openedCursors());
            }

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /** Fully executes multi-statements query without reading cursor data. */
    void executeScript(String query, Object ... params) {
        executeScript(query, null, params);
    }

    /** Fully executes multi-statements query without reading cursor data. */
    void executeScript(String query, @Nullable InternalTransaction tx, Object ... params) {
        iterateThroughResultsAndCloseThem(runScript(query, tx, params));
    }

    /** Initiates multi-statements query execution. */
    AsyncSqlCursor<InternalSqlRow> runScript(String query) {
        return runScript(query, null);
    }

    AsyncSqlCursor<InternalSqlRow> runScript(String query, @Nullable InternalTransaction tx, Object ... params) {
        SqlProperties properties = SqlPropertiesHelper.newBuilder()
                .set(QueryProperty.ALLOWED_QUERY_TYPES, SqlQueryType.ALL)
                .build();

        AsyncSqlCursor<InternalSqlRow> cursor = await(
                queryProcessor().queryAsync(properties, observableTimeTracker(), tx, query, params)
        );

        return Objects.requireNonNull(cursor);
    }

    static List<AsyncSqlCursor<InternalSqlRow>> fetchAllCursors(AsyncSqlCursor<InternalSqlRow> cursor) {
        return fetchCursors(cursor, -1, false);
    }

    static List<AsyncSqlCursor<InternalSqlRow>> fetchCursors(AsyncSqlCursor<InternalSqlRow> cursor, int count, boolean close) {
        List<AsyncSqlCursor<InternalSqlRow>> cursors = new ArrayList<>();

        cursors.add(cursor);

        if (close) {
            cursor.closeAsync();
        }

        while ((count < 0 || --count > 0) && cursor.hasNextResult()) {
            cursor = await(cursor.nextResult());

            assertNotNull(cursor);

            cursors.add(cursor);

            if (close) {
                cursor.closeAsync();
            }
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

        if (expected.length == 0) {
            assertThat(res.items(), empty());
        } else {
            List<InternalSqlRow> items = res.items();
            List<Object> rows = new ArrayList<>(items.size());
            for (InternalSqlRow item : items) {
                List<Object> row = new ArrayList<>(item.fieldCount());
                for (int i = 0; i < item.fieldCount(); i++) {
                    row.add(item.get(i));
                }
                rows.add(row);
            }

            assertEquals(List.of(List.of(expected)), rows);
        }

        assertFalse(res.hasMore());

        cursor.closeAsync();
    }
}
