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

package org.apache.ignite.internal.runner.app.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.ignite.internal.sql.engine.exec.ddl.DdlCommandHandler;
import org.junit.jupiter.api.Test;

/**
 * System columns self test.
 */
public class ItJdbcSystemColumnsSelfTest extends AbstractJdbcSelfTest {
    @Test
    public void testCreateTableWithSystemColumn() throws Exception {
        DdlCommandHandler.ENABLE_SYSTEM_COLUMNS_DIRECT_CREATION = false;

        assertThrows(SQLException.class,
                () -> stmt.executeUpdate("CREATE TABLE WRONG (ID INT, _KEY INT);"),
                "_KEY is a system column and should not be created directly.");
        assertThrows(SQLException.class,
                () -> stmt.executeUpdate("CREATE TABLE WRONG (ID INT, _key INT);"),
                "_key is a system column and should not be manipulated directly.");
    }

    @Test
    public void testAlterTableSystemColumn() throws Exception {
        DdlCommandHandler.ENABLE_SYSTEM_COLUMNS_DIRECT_CREATION = false;

        try (Statement s = conn.createStatement()) {
            s.executeUpdate("DROP TABLE IF EXISTS T1;");
            s.executeUpdate("CREATE TABLE T1(id1 INT PRIMARY KEY, id2 INT);");
        }

        assertThrows(SQLException.class,
                () -> stmt.executeUpdate("ALTER TABLE T1 ADD COLUMN(_KEY int);"),
                "_KEY is a system column and should not be manipulated directly.");

        assertThrows(SQLException.class,
                () -> stmt.executeUpdate("ALTER TABLE T1 DROP COLUMN _KEY;"),
                "_key is a system column and should not be manipulated directly.");
    }

    @Test
    public void testSelectWildcardWithoutSystemColumn() throws Exception {
        DdlCommandHandler.ENABLE_SYSTEM_COLUMNS_DIRECT_CREATION = true;

        try (Statement s = conn.createStatement()) {
            s.executeUpdate("DROP TABLE IF EXISTS T1;");
            s.executeUpdate("CREATE TABLE T1(id1 INT PRIMARY KEY, id2 INT, _KEY int);");
            s.executeUpdate("INSERT INTO T1(id1, id2, _KEY) VALUES (1,1,1),(2,2,2);");

            s.execute("SELECT * FROM T1;");

            ResultSet resultSet = s.getResultSet();

            assertTrue(resultSet.next());

            assertThrows(SQLException.class,
                    () -> resultSet.findColumn("_KEY"),
                    "_KEY column should not be found in final result.");

            s.execute("SELECT _KEY, * FROM T1;");

            ResultSet resultSet2 = s.getResultSet();

            assertTrue(resultSet2.next());

            assertEquals(1, resultSet2.findColumn("_KEY"));
            assertEquals(2, resultSet2.findColumn("id1"));
            assertEquals(3, resultSet2.findColumn("id2"));
        }
    }
}
