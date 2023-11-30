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

import static org.apache.ignite.internal.sql.engine.property.SqlPropertiesHelper.emptyProperties;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
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
        fetchCursors(runScript(query, null, params), -1, true);
    }

    /** Initiates multi-statements query execution. */
    AsyncSqlCursor<List<Object>> runScript(String query) {
        return runScript(query, null);
    }

    AsyncSqlCursor<List<Object>> runScript(String query, @Nullable InternalTransaction tx, Object ... params) {
        AsyncSqlCursor<List<Object>> cursor = await(
                queryProcessor().queryScriptAsync(emptyProperties(), igniteTx(), tx, query, params)
        );

        return Objects.requireNonNull(cursor);
    }

    static List<AsyncSqlCursor<List<Object>>> fetchAllCursors(AsyncSqlCursor<List<Object>> cursor) {
        return fetchCursors(cursor, -1, false);
    }

    static List<AsyncSqlCursor<List<Object>>> fetchCursors(AsyncSqlCursor<List<Object>> cursor, int count, boolean close) {
        List<AsyncSqlCursor<List<Object>>> cursors = new ArrayList<>();

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

    static void validateSingleResult(AsyncSqlCursor<List<Object>> cursor, Object... expected) {
        BatchedResult<List<Object>> res = await(cursor.requestNextAsync(1));
        assertNotNull(res);

        if (expected.length == 0) {
            assertThat(res.items(), empty());
        } else {
            assertThat(res.items(), equalTo(List.of(List.of(expected))));
        }

        assertFalse(res.hasMore());

        cursor.closeAsync();
    }
}
