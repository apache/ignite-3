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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.BatchUpdateException;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.raft.jraft.core.State;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Test;

/**
 * Statement batch test. Now it only checks if the data is sent
 * to the server correctly and if the error is correct.
 */
public class ItJdbcStatementBatchingSelfTest extends AbstractJdbcSelfTest {

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
    }

    /**
     * Test metadata batch support flag.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testDatabaseMetadataBatchSupportFlag() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        assertNotNull(meta);

        assertTrue(meta.supportsBatchUpdates());
    }

    @Test
    public void testBatch3() throws SQLException {
        try (Statement statement = conn.createStatement()) {
            statement.executeUpdate("CREATE TABLE Person(id1 INT PRIMARY KEY, id2 INT);");

            Statement statement1 = conn.createStatement();

            statement1.addBatch("INSERT INTO Person VALUES (1,1)");
            statement1.addBatch("INSERT INTO Person VALUES (2,2)");
            statement1.addBatch("INSERT INTO Person VALUES (3,3)");
            System.out.println(Arrays.toString(statement1.executeBatch()));
        }
    }

    /**
     * Test batch execution.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testBatch2() throws SQLException {
        try (Statement statement = conn.createStatement()) {
            statement.executeUpdate("CREATE TABLE Person(id1 INT PRIMARY KEY, id2 int);");
        }
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO Person(id1, id2) VALUES(?,?)")) {
            stmt.setInt(1, 1);
            stmt.setInt(2, 1);
            stmt.addBatch();
            stmt.setInt(1, 2);
            stmt.setInt(2, 2);
            stmt.addBatch();
            stmt.setInt(1, 3);
            stmt.setInt(2, 3);
            stmt.addBatch();

            int[] execute = stmt.executeBatch();
            System.out.println(Arrays.toString(execute));
        }
    }

    /**
     * Test batch execution.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testBatch() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE Person(id INT PRIMARY KEY, firstName VARCHAR, lastName VARCHAR, age INT);");

            stmt.addBatch("INSERT INTO Person(id, firstName, lastName, age) VALUES "
                    + "(1, 'John', 'White', 25), "
                    + "(2, 'Joe', 'Black', 35), "
                    + "(0, 'M', 'G', 4)");

            stmt.addBatch("UPDATE Person SET firstName = 'Mike', lastName = 'Green', "
                    + "age = 40 WHERE id = 2");

            stmt.addBatch("DELETE FROM Person WHERE id = 1");

            System.out.println(Arrays.toString(stmt.executeBatch()));
        }
    }
}
