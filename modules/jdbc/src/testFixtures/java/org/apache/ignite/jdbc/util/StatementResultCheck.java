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

package org.apache.ignite.jdbc.util;

import static org.apache.ignite.jdbc.util.RowColumnProjection.projectRowsColumn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Utility class for checking statement results after query execution.
 */
@FunctionalInterface
public interface StatementResultCheck {

    /**
     * Actual statement check. Call-site is responsible for calling {@link Statement#getMoreResults()} before this method.
     *
     * @param statement Statement that was used for query execution.
     * @throws SQLException If error occur during accessing statement result.
     */
    void check(Statement statement) throws SQLException;

    /** Assert that no more results are retrievable from this statement. */
    static StatementResultCheck noMoreResults() {
        return stmt -> {
            assertNull(stmt.getResultSet());
            assertEquals(-1, stmt.getUpdateCount());
        };
    }

    /** Assert that next result is update counter. */
    static StatementResultCheck isUpdateCounter(int expected) {
        return stmt -> {
            int updateCounter = stmt.getUpdateCount();
            assertEquals(expected, updateCounter, "Expected update counter equal to " + expected + ", but got " + updateCounter);
        };
    }

    /** Assert that next result is {@link ResultSet}. */
    static StatementResultCheck isResultSet() {
        return StatementResultCheck::assertRs;
    }

    /** Assert that next result is {@link ResultSet} with rows satisfying provided matcher. */
    static <T> StatementResultCheck isResultSet(RowColumnProjection<T> projection, RowsProjectionMatcher<T> matcher) {
        return stmt -> {
            ResultSet rs = assertRs(stmt);
            assertThat(projectRowsColumn(rs, projection), matcher);
        };
    }

    private static ResultSet assertRs(Statement statement) throws SQLException {
        ResultSet rs = statement.getResultSet();
        assertNotNull(rs, "Expected next ResultSet, but got <null>");

        return rs;
    }

}
