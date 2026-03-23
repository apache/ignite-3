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
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.sql.ResultSet;
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

    @Test
    void rootProjectionWithDuplicateNames() {
        String sql = "CREATE TABLE CITY(ID INT PRIMARY KEY, NAME VARCHAR(255));\n"
                + "CREATE TABLE STREET(ID INT PRIMARY KEY, CITY_ID INT, NAME VARCHAR(255));\n"
                + "INSERT INTO CITY(ID, NAME) VALUES(1, 'New York');\n"
                + "INSERT INTO STREET(ID, CITY_ID, NAME) VALUES(1, 1, 'Broadway');\n"
                + "INSERT INTO STREET(ID, CITY_ID, NAME) VALUES(2, 1, 'Wall Street');\n";

        sqlScript(sql);

        sql("SELECT CITY_ID, NAME, NAME FROM STREET ORDER BY ID");
        sql("SELECT * FROM STREET JOIN CITY ON STREET.CITY_ID = CITY.ID ORDER BY STREET.ID");
        sql("SELECT CITY.NAME, STREET.NAME FROM STREET JOIN CITY ON STREET.CITY_ID = CITY.ID ORDER BY STREET.ID");
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

        // for projection pop up
        ResultSet<SqlRow> res = igniteSql().execute("SELECT i, i_tz, id FROM timestamps");

        SqlRow row = res.next();

        assertEquals(localDate, row.datetimeValue("i"));
        assertEquals(instant, row.timestampValue("i_tz"));

        String query = "select \"KEY\", \"TIMESTAMP\", \"TIMESTAMP_TZ\" from TBL_ALL_COLUMNS_SQL ORDER BY KEY";

        res = igniteSql().execute(query);

        row = res.next();

        assertEquals(localDate, row.datetimeValue("TIMESTAMP"));
        assertEquals(instant, row.timestampValue("TIMESTAMP_TZ"));
    }
}
