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
import java.sql.SQLException;
import java.sql.Statement;
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

    /**
     * Test batch execution.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testBatch() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.addBatch("INSERT INTO Person(_key, id, firstName, lastName, age, data) "
                    + "VALUES ('p1', 0, 'J', 'W', 250, RAWTOHEX('W'))");

            stmt.addBatch("INSERT INTO Person(_key, id, firstName, lastName, age, data) VALUES "
                    + "('p1', 1, 'John', 'White', 25, RAWTOHEX('White')), "
                    + "('p2', 2, 'Joe', 'Black', 35, RAWTOHEX('Black')), "
                    + "('p3', 0, 'M', 'G', 4, RAWTOHEX('G'))");

            stmt.addBatch("UPDATE Person SET id = 3, firstName = 'Mike', lastName = 'Green', "
                    + "age = 40, data = RAWTOHEX('Green') WHERE _key = 'p3'");

            stmt.addBatch("DELETE FROM Person WHERE _key = 'p1'");

            assertThrows(BatchUpdateException.class, stmt::executeBatch,
                    "ExecuteBatch operation is not implemented yet.");
        }
    }
}
