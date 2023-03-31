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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.lang.ErrorGroups.Sql;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.schema.testutils.SchemaConfigurationConverter;
import org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders;
import org.apache.ignite.internal.schema.testutils.definition.ColumnType;
import org.apache.ignite.internal.schema.testutils.definition.ColumnType.TemporalColumnType;
import org.apache.ignite.internal.schema.testutils.definition.TableDefinition;
import org.apache.ignite.internal.sql.engine.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.exec.ExecutionCancelledException;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

/** Test common SQL API. */
public class ItCommonApiTest extends ClusterPerClassIntegrationTest {
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
        IgniteSql sql = igniteSql();

        sql("CREATE TABLE TST(id INTEGER PRIMARY KEY, val INTEGER)");
        sql("INSERT INTO TST VALUES (1,1), (2,2), (3,3), (4,4)");

        Session ses1 = sql.sessionBuilder().defaultPageSize(1).idleTimeout(2, TimeUnit.SECONDS).build();
        Session ses2 = sql.sessionBuilder().defaultPageSize(1).idleTimeout(100, TimeUnit.SECONDS).build();

        assertEquals(2, queryProcessor().liveSessions().size());

        ResultSet rs1 = ses1.execute(null, "SELECT id FROM TST");
        ResultSet rs2 = ses2.execute(null, "SELECT id FROM TST");

        waitForCondition(() -> {
            return queryProcessor().liveSessions().size() == 1;
        }, 10_000);

        // first session should be expired for the moment
        SqlException ex = assertThrows(SqlException.class, () -> ses1.execute(null, "SELECT 1 + 1"));
        assertEquals(Sql.SESSION_NOT_FOUND_ERR, ex.code());

        // already started query should fail due to session has been expired
        assertThrowsWithCause(() -> {
            while (rs1.hasNext()) {
                rs1.next();
            }
        }, ExecutionCancelledException.class);

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
        String schemaName = "PUBLIC";
        String keyCol = "key";
        int maxTimePrecision = TemporalColumnType.MAX_TIME_PRECISION;

        Ignite node = CLUSTER_NODES.get(0);

        // TODO: https://issues.apache.org/jira/browse/IGNITE-19162 Trim all less than millisecond information from timestamp
        //String tsStr = "2023-03-29T08:22:33.005007Z";
        String tsStr = "2023-03-29T08:22:33.005Z";

        Instant ins = Instant.parse(tsStr);

        sql("CREATE TABLE timestamps(id INTEGER PRIMARY KEY, i TIMESTAMP(9))");

        TableDefinition schTblAllSql = SchemaBuilders.tableBuilder(schemaName, kvTblName).columns(
                SchemaBuilders.column(keyCol, ColumnType.INT64).build(),
                SchemaBuilders.column("time", ColumnType.time(maxTimePrecision)).asNullable(true).build(),
                SchemaBuilders.column("timestamp", ColumnType.timestamp(maxTimePrecision)).asNullable(true).build(),
                SchemaBuilders.column("datetime", ColumnType.datetime(maxTimePrecision)).asNullable(true).build()
        ).withPrimaryKey(keyCol).build();

        await(((TableManager) node.tables()).createTableAsync(schTblAllSql.name(), tblCh ->
                SchemaConfigurationConverter.convert(schTblAllSql, tblCh)
        ));

        Table tbl = node.tables().table(kvTblName);

        LocalDateTime dateTime = LocalDateTime.of(2023, 1, 18, 18, 9, 29);

        Tuple rec = Tuple.create()
                .set("KEY", 1L)
                .set("TIMESTAMP", dateTime)
                .set("DATETIME", dateTime);

        tbl.recordView().insert(null, rec);

        // TODO: https://issues.apache.org/jira/browse/IGNITE-19161 Can`t insert timestamp representing in ISO_INSTANT format
        tsStr = tsStr.replace("T", " ").substring(0, tsStr.length() - 1);

        sql("INSERT INTO timestamps VALUES (101, TIMESTAMP '" + tsStr + "')");

        try (Session ses = node.sql().createSession()) {
            // for projection pop up
            ResultSet<SqlRow> res = ses.execute(null, "SELECT i, id FROM timestamps");

            assertEquals(ins.toString().replace("Z", ""), res.next().datetimeValue(0).toString());

            String query = "select \"KEY\", \"TIME\", \"DATETIME\", \"TIMESTAMP\" from TBL_ALL_COLUMNS_SQL ORDER BY KEY";

            res = node.sql().createSession().execute(null, query);

            SqlRow result = res.next();

            assertEquals(dateTime, result.datetimeValue(3));
            assertEquals(dateTime, result.datetimeValue(2));
        }
    }
}
