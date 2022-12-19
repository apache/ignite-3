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

package org.apache.ignite.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Consumer;
import org.apache.ignite.table.KeyValueView;
import org.junit.jupiter.api.Test;

/**
 * Update statement self test.
 */
public class ItJdbcUpdateStatementSelfTest extends AbstractJdbcSelfTest {
    /**
     * Execute test.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testExecuteUpdateMultipleColumnsUpdate() throws Exception {
        final String q1 = "CREATE TABLE usertable (\n"
                + "    ycsb_key1 int NOT NULL,\n"
                + "    ycsb_key2 varchar(3) NOT NULL,\n"
                + "    field1   varchar(100),\n"
                + "    field2   varchar(100),\n"
                + "    field3   varchar(100),\n"
                + "    field4   varchar(100),\n"
                + "    field5   varchar(100),\n"
                + "    field6   varchar(100),\n"
                + "    field7   varchar(100),\n"
                + "    field8   varchar(100),\n"
                + "    field9   varchar(100),\n"
                + "    field10  varchar(100),\n"
                + "    PRIMARY KEY(ycsb_key1, ycsb_key2)\n"
                + ");";

        final String q2 = "INSERT INTO usertable values(1, 'key', 'a1','a2','a3','a4','a5','a6','a7','a8','a9','a10');";

        final String q3 = "UPDATE usertable SET FIELD1='b1',FIELD2='b2',FIELD3='b3',FIELD4='b4',FIELD5='b5',"
                + "FIELD6='b6',FIELD7='b7',FIELD8='b8',FIELD9='b9',FIELD10='b10' WHERE YCSB_KEY1=1";

        final String q4 = "DROP TABLE usertable;";

        assertEquals(0, stmt.executeUpdate(q1));
        assertEquals(1, stmt.executeUpdate(q2));
        assertEquals(1, stmt.executeUpdate(q3));

        stmt.executeQuery("SELECT * FROM usertable WHERE YCSB_KEY1=1;");

        ResultSet resultSet = stmt.getResultSet();

        assertTrue(resultSet.next());

        for (int i = 1; i < 11; i++) {
            assertEquals("b" + i, resultSet.getString(i + 2));
        }

        assertEquals(0, stmt.executeUpdate(q4));
    }

    /**
     * Execute test.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testExecuteAndExecuteUpdate() throws SQLException {
        testExecute((String updateQuery) -> {
            try {
                stmt.executeUpdate(updateQuery);
            } catch (SQLException e) {
                fail(e);
            }
        });

        testExecute((String updateQuery) -> {
            try {
                stmt.execute(updateQuery);
            } catch (SQLException e) {
                fail(e);
            }
        });
    }

    private void testExecute(Consumer<String> updateFunction) throws SQLException {
        final String createSql = "CREATE TABLE public.person(id INTEGER PRIMARY KEY, sid VARCHAR,"
                + " firstname VARCHAR NOT NULL, lastname VARCHAR NOT NULL, age INTEGER NOT NULL)";

        final String insertSql = "INSERT INTO public.person(sid, id, firstname, lastname, age) VALUES "
                + "('p1', 1, 'John', 'White', 25), "
                + "('p2', 2, 'Joe', 'Black', 35), "
                + "('p3', 3, 'Mike', 'Green', 40)";

        final String updateSql = "update PUBLIC.PERSON set firstName = 'Jack' where substring(SID, 2, 1)::int % 2 = 0";

        final String dropSql = "DROP TABLE PUBLIC.PERSON;";

        updateFunction.accept(createSql);
        updateFunction.accept(insertSql);
        updateFunction.accept(updateSql);

        KeyValueView<Integer, Person> person = clusterNodes.get(0).tables()
                .table("PERSON").keyValueView(Integer.class, Person.class);

        assertEquals("John", person.get(null, 1).firstname);
        assertEquals("Jack", person.get(null, 2).firstname);
        assertEquals("Mike", person.get(null, 3).firstname);

        updateFunction.accept(dropSql);
    }

    private static class Person {
        public int age;
        public String sid;
        public String firstname;
        public String lastname;
    }
}
