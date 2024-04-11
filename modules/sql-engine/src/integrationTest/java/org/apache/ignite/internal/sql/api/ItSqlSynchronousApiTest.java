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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests for synchronous SQL API.
 */
public class ItSqlSynchronousApiTest extends ItSqlApiBaseTest {
    @Override
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18647")
    public void resultSetCloseShouldFinishImplicitTransaction() {
        super.resultSetCloseShouldFinishImplicitTransaction();
    }

    @Override
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18647")
    public void errors() throws InterruptedException {
        super.errors();
    }

    @Override
    protected ResultSet<SqlRow> executeForRead(IgniteSql sql, Transaction tx, Statement statement, Object... args) {
        return sql.execute(tx, statement, args);
    }

    @Override
    protected long[] executeBatch(IgniteSql sql, String query, BatchedArguments args) {
        return sql.executeBatch(null, query, args);
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
}
