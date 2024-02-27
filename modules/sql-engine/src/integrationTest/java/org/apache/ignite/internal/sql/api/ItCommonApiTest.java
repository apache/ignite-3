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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.lang.CursorClosedException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

/** Test common SQL API. */
public class ItCommonApiTest extends BaseSqlIntegrationTest {
    @Override
    protected int initialNodes() {
        return 1;
    }

    /** Check correctness of session expiration. */
    @Test
    public void testSessionExpiration() throws Exception {
        IgniteSql sql = igniteSql();

        sql("CREATE TABLE TST(id INTEGER PRIMARY KEY, val INTEGER)");
        sql("INSERT INTO TST VALUES (1,1), (2,2), (3,3), (4,4)");

        Session ses1 = sql.sessionBuilder().defaultPageSize(1).idleTimeout(2, TimeUnit.SECONDS).build();
        Session ses2 = sql.sessionBuilder().defaultPageSize(1).idleTimeout(100, TimeUnit.SECONDS).build();

        assertEquals(2, activeSessionsCount());

        ResultSet rs1 = ses1.execute(null, "SELECT id FROM TST");
        ResultSet rs2 = ses2.execute(null, "SELECT id FROM TST");

        assertTrue(waitForCondition(ses1::closed, 10_000));

        // first session should no longer exist for the moment
        ExecutionException err = assertThrows(ExecutionException.class, () -> ses1.executeAsync(null, "SELECT 1 + 1").get());
        assertThat(err.getCause(), instanceOf(IgniteException.class));
        assertThat(err.getCause().getMessage(), containsString("Session is closed"));

        // already started query should fail due to session has been expired
        assertThrowsWithCause(() -> {
            while (rs1.hasNext()) {
                rs1.next();
            }
        }, CursorClosedException.class);

        rs1.close();

        // second session could proceed with execution
        while (rs2.hasNext()) {
            rs2.next();
        }

        // second session could start new query
        ses2.execute(null, "SELECT 2 + 2").close();
    }

    /** Check timestamp type operations correctness using sql and kv api. */
    @Test
    public void checkTimestampOperations() {
        String kvTblName = "tbl_all_columns_sql";
        String keyCol = "KEY";

        Ignite node = CLUSTER.aliveNode();

        // TODO: https://issues.apache.org/jira/browse/IGNITE-19162 Trim all less than millisecond information from timestamp
        // String tsStr = "2023-03-29T08:22:33.005007Z";
        String tsStr = "2023-03-29T08:22:33.005Z";

        LocalDateTime localDate = LocalDateTime.parse(tsStr, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
        Instant instant = localDate.atZone(ZoneId.systemDefault()).toInstant();

        sql("CREATE TABLE timestamps(id INTEGER PRIMARY KEY, i TIMESTAMP(9), i_tz TIMESTAMP WITH LOCAL TIME ZONE)");

        sql(format("CREATE TABLE {}("
                        + "\"{}\" INTEGER PRIMARY KEY, "
                        + "\"TIMESTAMP\" TIMESTAMP(9), "
                        + "\"TIMESTAMP_TZ\" TIMESTAMP(9) WITH LOCAL TIME ZONE)", kvTblName, keyCol));

        Table tbl = node.tables().table(kvTblName);

        localDate = LocalDateTime.of(2023, 3, 29, 8, 22, 33, 5000000);

        Tuple rec = Tuple.create()
                .set("KEY", 1)
                .set("TIMESTAMP", localDate)
                .set("TIMESTAMP_TZ", instant);

        tbl.recordView().insert(null, rec);

        // TODO: https://issues.apache.org/jira/browse/IGNITE-19161 Can`t insert timestamp representing in ISO_INSTANT format
        String tsValue = tsStr.replace("T", " ").substring(0, tsStr.length() - 1);

        sql(format("INSERT INTO timestamps VALUES (101, TIMESTAMP '{}', TIMESTAMP WITH LOCAL TIME ZONE '{}')", tsValue, tsValue));

        try (Session ses = node.sql().createSession()) {
            // for projection pop up
            ResultSet<SqlRow> res = ses.execute(null, "SELECT i, i_tz, id FROM timestamps");

            SqlRow row = res.next();

            assertEquals(localDate, row.datetimeValue("i"));
            assertEquals(instant, row.timestampValue("i_tz"));

            String query = "select \"KEY\", \"TIMESTAMP\", \"TIMESTAMP_TZ\" from TBL_ALL_COLUMNS_SQL ORDER BY KEY";

            res = ses.execute(null, query);

            row = res.next();

            assertEquals(localDate, row.datetimeValue("TIMESTAMP"));
            assertEquals(instant, row.timestampValue("TIMESTAMP_TZ"));
        }
    }

    private int activeSessionsCount() {
        return ((IgniteSqlImpl) igniteSql()).sessions().size();
    }
}
