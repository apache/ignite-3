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

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsTableScan;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.ignite.lang.IndexAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Group of tests that still has not been sorted out. It’s better to avoid extending this class with new tests.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-15655")
public class ItIndexDdlTest extends AbstractBasicIntegrationTest {
    @Test
    public void indexBasic() {
        sql("create table test_tbl (id int primary key, val0 int, val1 varchar, val2 int)");

        sql("create index TEST_IDX on test_tbl (val1, val0)");

        insertData(
                "PUBLIC.TEST_TBL",
                new String[] {"ID", "VAL0", "VAL1", "VAL2"},
                new Object[] {0, 1, "val0", 0},
                new Object[] {1, 2, "val1", 1},
                new Object[] {2, 3, "val2", 2},
                new Object[] {3, null, "val3", 3}
        );

        // Scan index only
        assertQuery("SELECT VAL0, ID FROM test_tbl WHERE val0 > 1 and val1 > 'val' ORDER BY val1")
                .matches(containsIndexScan("PUBLIC", "TEST_TBL", "TEST_IDX"))
                .ordered()
                .returns(2, 1)
                .returns(3, 2)
                .check();

        // Scan index with lookup rows at the table
        assertQuery("SELECT * FROM test_tbl WHERE val0 > 1 and val1 > 'val' ORDER BY val1")
                .ordered()
                .matches(containsIndexScan("PUBLIC", "TEST_TBL", "TEST_IDX"))
                .returns(1, 2, "val1", 1)
                .returns(2, 3, "val2", 2)
                .check();

        sql("drop index TEST_IDX");

        assertQuery("SELECT * FROM test_tbl WHERE val0 > 1 and val1 > 'val' ORDER BY val1")
                .ordered()
                .matches(not(containsIndexScan("PUBLIC", "TEST_TBL", "TEST_IDX")))
                .matches(containsTableScan("PUBLIC", "TEST_TBL"))
                .returns(1, 2, "val1", 1)
                .returns(2, 3, "val2", 2)
                .check();
    }

    /**
     * Tests create /drop index through DDL.
     */
    @Test
    public void createDropIndex() {
        String tblName = "createDropIdx";

        String newTblSql = String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) with partitions=1", tblName);

        sql(newTblSql);

        sql(String.format("CREATE INDEX index1 ON %s (c1)", tblName));

        sql(String.format("CREATE INDEX IF NOT EXISTS index1 ON %s (c1)", tblName));

        sql(String.format("CREATE INDEX index2 ON %s (c1)", tblName));

        sql(String.format("CREATE INDEX index3 ON %s (c2)", tblName));

        assertThrows(IndexAlreadyExistsException.class, () ->
                sql(String.format("CREATE INDEX index3 ON %s (c1)", tblName)));

        assertThrows(TableNotFoundException.class, () ->
                sql(String.format("CREATE INDEX index_3 ON %s (c1)", tblName + "_nonExist")));

        sql(String.format("CREATE INDEX index4 ON %s (c2 desc, c1 asc)", tblName));

        sql(String.format("DROP INDEX index4"));

        sql(String.format("CREATE INDEX index4 ON %s (c2 desc, c1 asc)", tblName));

        sql(String.format("DROP INDEX index4"));

        sql(String.format("DROP INDEX IF EXISTS index4"));
    }
}
