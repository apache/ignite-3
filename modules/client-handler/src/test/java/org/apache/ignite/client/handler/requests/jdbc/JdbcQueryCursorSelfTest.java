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

package org.apache.ignite.client.handler.requests.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursorImpl;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.util.IteratorToDataCursorAdapter;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test class for {@link JdbcQueryCursor}.
 */
public class JdbcQueryCursorSelfTest extends BaseIgniteAbstractTest {
    private static final List<Integer> ROWS = List.of(1, 2, 3);

    private static final int TOTAL_ROWS_COUNT = ROWS.size();

    /** Tests corner cases of setting the {@code maxRows} parameter. */
    @ParameterizedTest(name = "maxRows={0}, fetchSize={1}")
    @MethodSource("maxRowsTestParameters")
    public void testMaxRows(int maxRows, int fetchSize) {
        List<Integer> results = fetchFullBatch(maxRows, fetchSize);
        List<Integer> expected = ROWS.subList(0, Math.min(maxRows, TOTAL_ROWS_COUNT));

        assertEquals(expected, results);
    }

    /** Tests cursor when the {@code maxRows} parameter is not defined (ie, set to {@code 0}). */
    @ParameterizedTest
    @ValueSource(ints = {1, 1024})
    public void testMaxRowsUndefined(int fetchSize) {
        List<Integer> results = fetchFullBatch(0, fetchSize);

        assertEquals(ROWS, results);
    }

    private List<Integer> fetchFullBatch(int maxRows, int fetchSize) {
        JdbcQueryCursor<Integer> cursor = new JdbcQueryCursor<>(maxRows,
                new AsyncSqlCursorImpl<>(
                        SqlQueryType.QUERY,
                        null,
                        new IteratorToDataCursorAdapter<>(CompletableFuture.completedFuture(ROWS.iterator()), Runnable::run),
                        null
                )
        );

        List<Integer> results = new ArrayList<>(maxRows);
        BatchedResult<Integer> requestResult;

        do {
            requestResult = cursor.requestNextAsync(fetchSize).join();

            results.addAll(requestResult.items());
        } while (requestResult.hasMore());

        return results;
    }

    private static List<Arguments> maxRowsTestParameters() {
        int total = TOTAL_ROWS_COUNT;
        int bigger = total + 1;
        int smaller = total - 1;

        return List.of(
                Arguments.of(smaller, 1024),
                Arguments.of(total, 1024),
                Arguments.of(bigger, 1024),

                Arguments.of(smaller, 1),
                Arguments.of(total, 1),
                Arguments.of(bigger, 1),

                Arguments.of(smaller, smaller),
                Arguments.of(total, total),
                Arguments.of(bigger, bigger),

                Arguments.of(smaller, smaller + 1),
                Arguments.of(total, total + 1),
                Arguments.of(bigger, bigger + 1)
        );
    }
}
