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

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Check LIMIT and\or OFFSET commands.
 */
public class ItLimitOffsetTest extends BaseSqlIntegrationTest {
    private static final String TABLE_NAME = "test";

    @BeforeEach
    void beforeEach() {
        sql("CREATE TABLE " + TABLE_NAME + " (pk INT PRIMARY KEY, col0 INT)");
    }

    @AfterEach
    void afterEach() {
        sql("DROP TABLE IF EXISTS test");
    }

    /** Tests correctness of fetch / offset params. */
    @Test
    public void testInvalidLimitOffset() {
        BigDecimal moreThanUpperLong = new BigDecimal(Long.MAX_VALUE).add(new BigDecimal(1));

        // cache the plan with concrete type param
        igniteSql().execute("SELECT * FROM test OFFSET ? ROWS", new BigDecimal(Long.MAX_VALUE));

        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "Illegal value of offset",
                () -> igniteSql().execute("SELECT * FROM test OFFSET ? ROWS", new BigDecimal(-1)));

        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "Illegal value of offset",
                () -> igniteSql().execute("SELECT * FROM test OFFSET ? ROWS", -1));

        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "Illegal value of offset",
                () -> igniteSql().execute("SELECT * FROM test OFFSET ? ROWS", (Object) null));

        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "Illegal value of offset",
                () -> igniteSql().execute("SELECT * FROM test OFFSET ? ROWS", moreThanUpperLong));

        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "Illegal value of offset",
                () -> igniteSql().execute("SELECT * FROM test OFFSET ? ROWS", (double) Long.MAX_VALUE + 1));

        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "Illegal value of offset",
                () -> igniteSql().execute("SELECT * FROM test OFFSET ? ROWS", (float) Long.MAX_VALUE + 1));

        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "Illegal value of offset",
                () -> igniteSql().execute("SELECT * FROM test OFFSET " + moreThanUpperLong + " ROWS"));

        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "Illegal value of fetch / limit",
                () -> igniteSql().execute("SELECT * FROM test FETCH FIRST " + moreThanUpperLong + " ROWS ONLY"));

        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "Illegal value of fetch / limit",
                () -> igniteSql().execute("SELECT * FROM test LIMIT " + moreThanUpperLong));

        assertThrowsSqlException(Sql.STMT_PARSE_ERR,
                "Failed to parse query: Encountered \"-\"",
                () -> igniteSql().execute("SELECT * FROM test OFFSET -1 ROWS FETCH FIRST -1 ROWS ONLY"));

        assertThrowsSqlException(Sql.STMT_PARSE_ERR,
                "Failed to parse query: Encountered \"-\"",
                () -> igniteSql().execute("SELECT * FROM test OFFSET -1 ROWS"));

        assertThrowsSqlException(Sql.STMT_PARSE_ERR, "Failed to parse query: Encountered \"-\"",
                () -> igniteSql().execute("SELECT * FROM test FETCH FIRST -1 ROWS ONLY"));

        assertThrowsSqlException(Sql.STMT_PARSE_ERR,
                "Failed to parse query: Encountered \"+\"",
                () -> igniteSql().execute("SELECT * FROM test OFFSET 2+1 ROWS"));
    }

    /**
     * Check execution correctness.
     */
    @Test
    public void testLimitOffset() {
        int inBufSize = Commons.IN_BUFFER_SIZE;

        int[] rowsArr = {10, inBufSize, (2 * inBufSize) - 1};

        for (int rows : rowsArr) {
            List<List<Object>> res = sql("SELECT COUNT(*) FROM test");

            long count = (long) res.get(0).get(0);

            for (long i = count; i < rows; ++i) {
                sql(String.format("INSERT INTO test VALUES(%d, %d);", i, i));
            }

            int[] limits = {-1, 0, 10, rows / 2 - 1, rows / 2, rows / 2 + 1, rows - 1, rows};
            int[] offsets = {-1, 0, 10, rows / 2 - 1, rows / 2, rows / 2 + 1, rows - 1, rows};

            for (int lim : limits) {
                for (int off : offsets) {
                    log.info("Check [rows=" + rows + ", limit=" + lim + ", off=" + off + ']');

                    checkQuery(rows, lim, off, false, false);
                    checkQuery(rows, lim, off, true, false);
                    checkQuery(rows, lim, off, false, true);
                    checkQuery(rows, lim, off, true, true);
                }
            }
        }
    }

    /** Check correctness of row count estimation. */
    @Test
    public void testOffsetOutOfRange() {
        for (long i = 0; i < 5; ++i) {
            sql(String.format("INSERT INTO test VALUES(%d, %d);", i, i));
        }

        assertQuery("SELECT (SELECT pk FROM test ORDER BY pk LIMIT 1 OFFSET 10)").returns(new Object[]{null}).check();
    }

    /**
     * Check query with specified limit and offset.
     *
     * @param rows Rows count.
     * @param lim Limit.
     * @param off Offset.
     * @param param If {@code false} place limit/offset as literals, otherwise they are placed as parameters.
     * @param sorted Use sorted query (adds ORDER BY).
     */
    void checkQuery(int rows, int lim, int off, boolean param, boolean sorted) {
        String request = createSql(lim, off, param, sorted);

        Object[] params = null;
        if (param) {
            if (lim >= 0 && off >= 0) {
                params = new Object[]{off, lim};
            } else if (lim >= 0) {
                params = new Object[]{lim};
            } else if (off >= 0) {
                params = new Object[]{off};
            }
        }

        log.info("SQL: " + request + (param ? "params=" + Arrays.toString(params) : ""));

        List<List<Object>> res = params != null ? sql(request, params) : sql(request);

        assertEquals(expectedSize(rows, lim, off), res.size(), "Invalid results size. [rows=" + rows + ", limit=" + lim + ", off=" + off
                + ", res=" + res.size() + ']');
    }

    /**
     * Calculates expected result set size by limit and offset.
     */
    private int expectedSize(int rows, int lim, int off) {
        if (off < 0) {
            off = 0;
        }

        if (lim == 0) {
            return 0;
        } else if (lim < 0) {
            return rows - off;
        } else if (lim + off < rows) {
            return lim;
        } else if (off > rows) {
            return 0;
        } else {
            return rows - off;
        }
    }

    /**
     * Form sql request according to incoming parameters.
     *
     * @param lim Limit.
     * @param off Offset.
     * @param param Flag to place limit/offset  by parameter or literal.
     * @return SQL query string.
     */
    private String createSql(int lim, int off, boolean param, boolean sorted) {
        StringBuilder sb = new StringBuilder("SELECT * FROM test ");

        if (sorted) {
            sb.append("ORDER BY pk ");
        }

        if (off >= 0) {
            sb.append("OFFSET ").append(param ? "?" : Integer.toString(off)).append(" ROWS ");
        }

        if (lim >= 0) {
            sb.append("FETCH FIRST ").append(param ? "?" : Integer.toString(lim)).append(" ROWS ONLY ");
        }

        return sb.toString();
    }
}
