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

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Test;

/**
 * Tests for synchronous SQL API.
 */
public class ItSqlSynchronousApiTest extends ItSqlApiBaseTest {
    @Override
    protected ResultSet<SqlRow> executeForRead(IgniteSql sql, Transaction tx, Statement statement, Object... args) {
        return sql.execute(tx, statement, args);
    }

    @Override
    protected long[] executeBatch(String query, BatchedArguments args) {
        return igniteSql().executeBatch(null, query, args);
    }

    @Override
    protected long[] executeBatch(Statement statement, BatchedArguments args) {
        return igniteSql().executeBatch(null, statement, args);
    }

    @Override
    protected ResultProcessor execute(Integer expectedPages, Transaction tx, IgniteSql sql, Statement statement, Object... args) {
        SyncPageProcessor syncProcessor = new SyncPageProcessor();

        ResultSet<SqlRow> rs = sql.execute(tx, statement, args);
        syncProcessor.process(rs);

        return syncProcessor;
    }

    protected ResultProcessor execute(int expectedPages, Transaction tx, IgniteSql sql, String query, Object... args) {
        SyncPageProcessor syncProcessor = new SyncPageProcessor();

        ResultSet<SqlRow> rs = sql.execute(tx, query, args);
        syncProcessor.process(rs);

        return syncProcessor;
    }

    @Override
    protected void executeScript(IgniteSql sql, String query, Object... args) {
        sql.executeScript(query, args);
    }

    @Override
    protected void rollback(Transaction tx) {
        tx.rollback();
    }

    @Override
    protected void commit(Transaction tx) {
        tx.commit();
    }

    @Override
    protected void checkDml(int expectedAffectedRows, Transaction tx, IgniteSql sql, String query, Object... args) {
        ResultSet res = sql.execute(
                tx,
                query,
                args
        );

        assertFalse(res.wasApplied());
        assertFalse(res.hasRowSet());
        assertEquals(expectedAffectedRows, res.affectedRows());

        res.close();
    }

    @Override
    protected void checkDdl(boolean expectedApplied, IgniteSql sql, String query, Transaction tx) {
        ResultSet res = sql.execute(
                tx,
                query
        );

        assertEquals(expectedApplied, res.wasApplied());
        assertFalse(res.hasRowSet());
        assertEquals(-1, res.affectedRows());

        res.close();
    }

    static class SyncPageProcessor implements ResultProcessor {
        private final List<SqlRow> res = new ArrayList<>();
        private long affectedRows;

        @Override
        public List<SqlRow> result() {
            //noinspection AssignmentOrReturnOfFieldWithMutableType
            return res;
        }

        @Override
        public long affectedRows() {
            return affectedRows;
        }

        public void process(ResultSet<SqlRow> resultSet) {
            affectedRows = resultSet.affectedRows();
            resultSet.forEachRemaining(res::add);
        }
    }

    @Test
    public void testEarlyQueryTimeout() {
        Statement stmt = igniteSql().statementBuilder()
                .query("SELECT * FROM TABLE(SYSTEM_RANGE(1, 1000000000000000))")
                .queryTimeout(1, TimeUnit.MILLISECONDS)
                .build();

        // Do not have enough time to do anything.
        assertThrowsSqlException(Sql.EXECUTION_CANCELLED_ERR, "Query timeout", () -> {
            igniteSql().execute(null, stmt);
        });
    }

    @Test
    public void testQueryTimeout() {
        Statement stmt = igniteSql().statementBuilder()
                .query("SELECT * FROM TABLE(SYSTEM_RANGE(1, 1000000000000000))")
                .queryTimeout(100, TimeUnit.MILLISECONDS)
                .build();

        ResultSet<SqlRow> rs;
        while (true) {
            try {
                rs = igniteSql().execute(null, stmt);
                break;
            } catch (SqlException e) {
                if (e.code() == Sql.PLANNING_TIMEOUT_ERR || e.code() == Sql.EXECUTION_CANCELLED_ERR) {
                    continue;
                }
                fail(e.getMessage());
            }
        }
        ResultSet<SqlRow> resultSet = rs;
        assertNotNull(resultSet);

        // Read data until exception occurs.
        assertThrowsSqlException(Sql.EXECUTION_CANCELLED_ERR, "Query timeout", () -> {
            while (resultSet.hasNext()) {
                resultSet.next();
            }
        });
    }

    @Test
    public void testQueryTimeoutIsPropagatedFromTheServer() throws Exception {
        Statement stmt = igniteSql().statementBuilder()
                .query("SELECT * FROM TABLE(SYSTEM_RANGE(1, 1000000000000000))")
                .queryTimeout(100, TimeUnit.MILLISECONDS)
                .build();

        ResultSet<SqlRow> rs;
        while (true) {
            try {
                rs = igniteSql().execute(null, stmt);
                break;
            } catch (SqlException e) {
                if (e.code() == Sql.PLANNING_TIMEOUT_ERR || e.code() == Sql.EXECUTION_CANCELLED_ERR) {
                    continue;
                }
                fail(e.getMessage());
            }
        }
        ResultSet<SqlRow> resultSet = rs;

        assertNotNull(resultSet);
        assertTrue(resultSet.hasNext());
        assertNotNull(resultSet.next());

        // wait sometime until the time is right for a timeout to occur.
        // then start retrieving the remaining data to trigger timeout exception.
        TimeUnit.SECONDS.sleep(2);

        assertThrowsSqlException(Sql.EXECUTION_CANCELLED_ERR, "Query timeout", () -> {
            while (resultSet.hasNext()) {
                resultSet.next();
            }
        });
    }

    @Test
    public void testDdlTimeout() {
        IgniteSql igniteSql = igniteSql();
        int timeoutMillis = 5;

        Statement stmt = igniteSql.statementBuilder()
                .query("CREATE TABLE test (ID INT PRIMARY KEY, VAL0 INT)")
                .queryTimeout(timeoutMillis, TimeUnit.MILLISECONDS)
                .build();

        // Trigger query timeout from the planner or the parser.
        assertThrowsSqlException(Sql.EXECUTION_CANCELLED_ERR, "Query timeout", () -> {
            igniteSql.execute(null, stmt);
        });
    }
}
