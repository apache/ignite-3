/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.sql.engine.AbstractBasicIntegrationTest;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.sql.CursorClosedException;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.Test;

/** Test common SQL API. */
public class ItCommonApiTest extends AbstractBasicIntegrationTest {
    protected SqlQueryProcessor queryProcessor() {
        return (SqlQueryProcessor) ((IgniteImpl) CLUSTER_NODES.get(0)).queryEngine();
    }

    /**
     * Gets the SQL API.
     *
     * @return SQL API.
     */
    protected IgniteSql igniteSql() {
        return CLUSTER_NODES.get(0).sql();
    }

    @Override
    protected int nodes() {
        return 1;
    }

    /** Check correctness of session expiration. */
    @Test
    public void testSessionExpiration() throws Exception {
        long timeout = TimeUnit.SECONDS.toMillis(2); // time from SessionManager.checkPeriod * 2

        IgniteSql sql = igniteSql();

        sql("CREATE TABLE TST(id INTEGER PRIMARY KEY, val INTEGER)");
        sql("INSERT INTO TST VALUES (1,1), (2,2), (3,3), (4,4)");

        Session ses1 = sql.sessionBuilder().defaultPageSize(1).defaultIdleSessionTimeout(1, TimeUnit.MILLISECONDS).build();
        Session ses2 = sql.sessionBuilder().defaultPageSize(1).defaultIdleSessionTimeout(100, TimeUnit.SECONDS).build();

        ResultSet rs1 = ses1.execute(null, "SELECT id FROM TST");
        ResultSet rs2 = ses2.execute(null, "SELECT id FROM TST");

        // waiting for run session cleanup thread
        Thread.sleep(timeout);

        // first session should be expired for the moment
        SqlException ex = assertThrows(SqlException.class, () -> ses1.execute(null, "SELECT 1 + 1"));
        assertTrue(ex.getMessage().contains("IGN-SQL-2"));

        // already started query should fail due to session has been expired
        ex = assertThrows(CursorClosedException.class, () -> {
            while (rs1.hasNext()) {
                rs1.next();
            }
        });

        rs1.close();

        assertTrue(ex.getMessage().contains("IGN-SQL-9"));

        // second session could proceed with execution
        while (rs2.hasNext()) {
            rs2.next();
        }

        // second session could start new query
        ses2.execute(null, "SELECT 2 + 2").close();
    }
}
