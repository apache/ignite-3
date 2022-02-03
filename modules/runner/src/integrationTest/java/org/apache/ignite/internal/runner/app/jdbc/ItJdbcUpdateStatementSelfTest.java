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

import java.sql.SQLException;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Update statement self test.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-15655")
public class ItJdbcUpdateStatementSelfTest extends AbstractJdbcSelfTest {
    /**
     * Execute test.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testExecuteAndExecuteUpdate() throws SQLException {
        final String createSql = "CREATE TABLE public.person(id INTEGER PRIMARY KEY, sid VARCHAR,"
                + " firstname VARCHAR NOT NULL, lastname VARCHAR NOT NULL, age INTEGER NOT NULL)";

        final String insertSql = "INSERT INTO public.person(sid, id, firstname, lastname, age) VALUES "
                + "('p1', 1, 'John', 'White', 25), "
                + "('p2', 2, 'Joe', 'Black', 35), "
                + "('p3', 3, 'Mike', 'Green', 40)";

        final String updateSql = "update PUBLIC.PERSON set firstName = 'Jack' where substring(SID, 2, 1)::int % 2 = 0";

        stmt.executeUpdate(createSql);
        stmt.executeUpdate(insertSql);
        stmt.execute(updateSql);

        KeyValueView<Tuple, Tuple> person = clusterNodes.get(0).tables()
                .table("PUBLIC.PERSON").keyValueView();

        assertEquals("John", person.get(null, Tuple.create().set("ID", 1)).stringValue("FIRSTNAME"));
        assertEquals("Jack", person.get(null, Tuple.create().set("ID", 2)).stringValue("FIRSTNAME"));
        assertEquals("Mike", person.get(null, Tuple.create().set("ID", 3)).stringValue("FIRSTNAME"));

        final String updateSql2 = "update PUBLIC.PERSON set firstName = 'Merlin' where substring(SID, 2, 1)::int % 2 = 0";

        stmt.executeUpdate(updateSql2);

        assertEquals("Merlin", person.get(null, Tuple.create().set("ID", 2)).stringValue("FIRSTNAME"));
    }

    /**
     * Execute test.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testExecute() throws SQLException {
        final String createSql = "CREATE TABLE public.person(id INTEGER PRIMARY KEY, sid VARCHAR,"
                + " firstname VARCHAR NOT NULL, lastname VARCHAR NOT NULL, age INTEGER NOT NULL)";

        final String insertSql = "INSERT INTO public.person(sid, id, firstname, lastname, age) VALUES "
                + "('p1', 1, 'John', 'White', 25), "
                + "('p2', 2, 'Joe', 'Black', 35), "
                + "('p3', 3, 'Mike', 'Green', 40)";

        final String updateSql = "update PUBLIC.PERSON set firstName = 'Jack' where substring(SID, 2, 1)::int % 2 = 0";

        stmt.executeUpdate(createSql);
        stmt.executeUpdate(insertSql);
        stmt.executeUpdate(updateSql);

        KeyValueView<Tuple, Tuple> person = clusterNodes.get(0).tables()
                .table("PUBLIC.PERSON").keyValueView();

        assertEquals("John", person.get(null, Tuple.create().set("ID", 1)).stringValue("FIRSTNAME"));
        assertEquals("Jack", person.get(null, Tuple.create().set("ID", 2)).stringValue("FIRSTNAME"));
        assertEquals("Mike", person.get(null, Tuple.create().set("ID", 3)).stringValue("FIRSTNAME"));
    }

    /**
     * Execute test.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testDifferentTableStructures() throws SQLException {
        final String createSql = "CREATE TABLE public.tbl1(id INTEGER PRIMARY KEY, id2 INTEGER NOT NULL, id3 INTEGER NOT NULL)";

        final String insertSql = "INSERT INTO public.tbl1(id, id2, id3) VALUES "
                + "(1, 25, 99), "
                + "(2, 35, 99), "
                + "(3, 40, 99)";

        final String updateSql = "update PUBLIC.tbl1 set id2 = 42 where id = 1;";

        stmt.executeUpdate(createSql);
        stmt.executeUpdate(insertSql);
        stmt.executeUpdate(updateSql);

        KeyValueView<Tuple, Tuple> tbl1 = clusterNodes.get(0).tables()
                .table("PUBLIC.tbl1").keyValueView();

        assertEquals(42, tbl1.get(null, Tuple.create().set("ID", 1)).intValue("id2"));
    }

    /**
     * Execute test.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testDifferentTableStructures2() throws SQLException {
        final String createSql = "CREATE TABLE public.tbl1(id VARCHAR PRIMARY KEY, id2 INTEGER NOT NULL, id3 INTEGER NOT NULL)";

        final String insertSql = "INSERT INTO public.tbl1(id, id2, id3) VALUES "
                + "('1', 25, 99), "
                + "('2', 35, 99), "
                + "('3', 40, 99)";

        final String updateSql = "update PUBLIC.tbl1 set id2 = 42 where id = '1';";

        stmt.executeUpdate(createSql);
        stmt.executeUpdate(insertSql);
        stmt.executeUpdate(updateSql);

        KeyValueView<Tuple, Tuple> tbl1 = clusterNodes.get(0).tables()
                .table("PUBLIC.tbl1").keyValueView();

        assertEquals(42, tbl1.get(null, Tuple.create().set("ID", "1")).intValue("id2"));
    }

    /**
     * Execute test.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testDifferentTableStructures3() throws SQLException {
        final String createSql = "CREATE TABLE public.tbl1(id VARCHAR PRIMARY KEY, id2 VARCHAR NOT NULL, id3 VARCHAR NOT NULL)";

        final String insertSql = "INSERT INTO public.tbl1(id, id2, id3) VALUES "
                + "('1', '25', '99'), "
                + "('2', '35', '99'), "
                + "('3', '40', '99')";

        final String updateSql = "update PUBLIC.tbl1 set id2 = '42' where id = '1';";

        stmt.executeUpdate(createSql);
        stmt.executeUpdate(insertSql);
        stmt.executeUpdate(updateSql);

        KeyValueView<Tuple, Tuple> tbl1 = clusterNodes.get(0).tables()
                .table("PUBLIC.tbl1").keyValueView();

        assertEquals("42", tbl1.get(null, Tuple.create().set("ID", "1")).stringValue("id2"));
    }
}
