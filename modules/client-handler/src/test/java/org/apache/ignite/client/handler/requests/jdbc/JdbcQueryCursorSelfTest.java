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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.sql.engine.AsyncCursor.BatchedResult;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursorImpl;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.exec.AsyncWrapper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for for {@link JdbcQueryCursor}.
 */
public class JdbcQueryCursorSelfTest {
    private static final int TOTAL_ROWS_COUNT = 3;

    /** Basic test to check corner cases of setting the {@code maxRows} parameter. */
    @ParameterizedTest(name = "maxRows={0}, fetchSize={1}")
    @MethodSource("maxRowsTestParameters")
    public void testMaxRows(int maxRows, int fetchSize) {
        List<Integer> rows = IntStream.range(0, TOTAL_ROWS_COUNT).boxed().collect(Collectors.toList());

        JdbcQueryCursor<Integer> cursor = new JdbcQueryCursor<>(maxRows,
                new AsyncSqlCursorImpl<>(SqlQueryType.QUERY, null, null,
                        new AsyncWrapper<>(CompletableFuture.completedFuture(rows.iterator()), Runnable::run)));


        List<Integer> results = new ArrayList<>(maxRows);
        BatchedResult<Integer> requestResult;

        do {
            requestResult = cursor.requestNextAsync(fetchSize).join();

            results.addAll(requestResult.items());
        } while (requestResult.hasMore());

        if (maxRows == 0 || maxRows >= TOTAL_ROWS_COUNT) {
            assertEquals(rows, results);
        } else {
            assertEquals(rows.subList(0, maxRows), results);
        }
    }

    private static List<Arguments> maxRowsTestParameters() {
        int[] fetchSizes = {1024, 1, 0, -1};
        int[] maxRowsSizes = {TOTAL_ROWS_COUNT - 1, TOTAL_ROWS_COUNT, TOTAL_ROWS_COUNT + 1};

        List<Arguments> args = new ArrayList<>();

        args.add(Arguments.of(0, 1024));
        args.add(Arguments.of(0, 1));

        for (int fetchSize : fetchSizes) {
            for (int maxRows : maxRowsSizes) {
                if (fetchSize <= 0) {
                    args.add(Arguments.of(maxRows, fetchSize == 0 ? maxRows : maxRows + Math.abs(fetchSize)));

                    continue;
                }

                args.add(Arguments.of(maxRows, fetchSize));
            }
        }

        return args;
    }
}
