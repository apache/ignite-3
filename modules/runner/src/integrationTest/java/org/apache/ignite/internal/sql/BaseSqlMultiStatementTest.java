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

package org.apache.ignite.internal.sql;

import static org.apache.ignite.internal.sql.engine.property.SqlPropertiesHelper.emptyProperties;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;

/**
 * Base class for SQL multi-statement queries integration tests.
 */
public abstract class BaseSqlMultiStatementTest extends BaseSqlIntegrationTest {
    @AfterEach
    protected void checkNoPendingTransactions() {
        assertEquals(0, txManager().pending());
    }

    protected AsyncSqlCursor<List<Object>> runScript(String query) {
        return runScript(query, null);
    }

    protected AsyncSqlCursor<List<Object>> runScript(
            String query,
            @Nullable InternalTransaction tx,
            Object ... params
    ) {
        AsyncSqlCursor<List<Object>> cursor =
                await(queryProcessor().queryScriptAsync(emptyProperties(), igniteTx(), tx, query, params));

        return Objects.requireNonNull(cursor);
    }

    protected static List<AsyncSqlCursor<List<Object>>> fetchAllCursors(AsyncSqlCursor<List<Object>> cursor) {
        List<AsyncSqlCursor<List<Object>>> cursors = new ArrayList<>();

        cursors.add(cursor);

        while (cursor.hasNextResult()) {
            cursor = await(cursor.nextResult());

            assertNotNull(cursor);

            cursors.add(cursor);
        }

        return cursors;
    }

    protected static void validateSingleResult(AsyncSqlCursor<List<Object>> cursor, Object... expected) {
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
