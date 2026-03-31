package org.apache.ignite.jdbc.util;

import static org.apache.ignite.jdbc.util.RowColumnProjection.projectRowsColumn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@FunctionalInterface
public interface StatementResultCheck {

    void check(Statement statement) throws SQLException;

    default StatementResultCheck and(StatementResultCheck nextCheck) {
        return statement -> {
            this.check(statement);
            nextCheck.check(statement);
        };
    }

    static StatementResultCheck noMoreResults() {
        return stmt -> {
            assertNull(stmt.getResultSet());
            assertEquals(-1, stmt.getUpdateCount());
        };
    }

    static StatementResultCheck isUpdateCounter(int expected) {
        return stmt -> {
            int updateCounter = stmt.getUpdateCount();
            assertEquals(expected, updateCounter, "Expected update counter equal to " + expected + ", but got " + updateCounter);
        };
    }

    static StatementResultCheck isResultSet() {
        return StatementResultCheck::assertRs;
    }

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
