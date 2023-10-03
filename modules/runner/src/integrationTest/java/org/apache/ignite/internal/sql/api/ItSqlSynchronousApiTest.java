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
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.tx.Transaction;

/**
 * Tests for synchronous SQL API.
 */
@SuppressWarnings("ThrowableNotThrown")
public class ItSqlSynchronousApiTest extends ItSqlApiBaseTest {
    @Override
    protected ResultSet<SqlRow> executeForRead(Session ses, Transaction tx, String query) {
        return ses.execute(tx, query);
    }

    @Override
    protected long[] executeBatch(Session ses, String sql, BatchedArguments args) {
        return ses.executeBatch(null, sql, args);
    }


    @Override
    protected ResultProcessor execute(Integer expectedPages, Transaction tx, Session ses, String sql, Object... args) {
        SyncPageProcessor syncProcessor = new SyncPageProcessor();

        ResultSet<SqlRow> rs = ses.execute(tx, sql, args);
        syncProcessor.process(rs);

        return syncProcessor;
    }

    protected ResultProcessor execute(int expectedPages, Transaction tx, Session ses, String sql, Object... args) {
        SyncPageProcessor syncProcessor = new SyncPageProcessor();

        ResultSet<SqlRow> rs = ses.execute(tx, sql, args);
        syncProcessor.process(rs);

        return syncProcessor;
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
    protected void checkDml(int expectedAffectedRows, Transaction tx, Session ses, String sql, Object... args) {
        ResultSet res = ses.execute(
                tx,
                sql,
                args
        );

        assertFalse(res.wasApplied());
        assertFalse(res.hasRowSet());
        assertEquals(expectedAffectedRows, res.affectedRows());

        res.close();
    }

    @Override
    protected void checkDdl(boolean expectedApplied, Session ses, String sql, Transaction tx) {
        ResultSet res = ses.execute(
                tx,
                sql
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
