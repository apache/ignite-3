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

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;

import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * SQL tests for schema DDL commands.
 */
public class ItSchemaTest extends BaseSqlIntegrationTest {

    @AfterEach
    public void dropSchemas() {
        dropAllSchemas();
    }

    @Test
    public void createSchema() {
        sql("CREATE SCHEMA IF NOT EXISTS schema1");

        sql("CREATE TABLE schema1.test1 (id INT PRIMARY KEY, val INT)");
        sql("INSERT INTO schema1.test1 VALUES (1, 1), (2, 2)");

        assertQuery("SELECT * FROM schema1.test1")
                .returnRowCount(2)
                .check();

        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                "Schema with name 'SCHEMA1' already exists.",
                () -> sql("CREATE SCHEMA schema1")
        );

        // Table is still accessible
        assertQuery("SELECT * FROM schema1.test1")
                .returnRowCount(2)
                .check();
    }

    @Test
    public void createSchemaIfExists() {
        sql("CREATE SCHEMA IF NOT EXISTS schema1");

        sql("CREATE TABLE schema1.test1 (id INT PRIMARY KEY, val INT)");
        sql("INSERT INTO schema1.test1 VALUES (1, 1), (2, 2)");

        assertQuery("SELECT * FROM schema1.test1")
                .returnRowCount(2)
                .check();

        sql("CREATE SCHEMA IF NOT EXISTS schema1");

        assertQuery("SELECT * FROM schema1.test1")
                .returnRowCount(2)
                .check();
    }

    @Test
    public void dropSchemaDefaultBehaviour() {
        sql("CREATE SCHEMA schema1");
        sql("CREATE TABLE schema1.test1 (id INT PRIMARY KEY, val INT)");

        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                "Schema 'SCHEMA1' is not empty. Use CASCADE to drop it anyway.",
                () -> sql("DROP SCHEMA schema1")
        );

        // Succeeds
        sql("DROP TABLE schema1.test1");
        sql("DROP SCHEMA schema1");
    }

    @Test
    public void dropSchemaRestrict() {
        sql("CREATE SCHEMA schema1");
        sql("CREATE TABLE schema1.test1 (id INT PRIMARY KEY, val INT)");

        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                "Schema 'SCHEMA1' is not empty. Use CASCADE to drop it anyway.",
                () -> sql("DROP SCHEMA schema1 RESTRICT")
        );

        // Succeeds
        sql("DROP TABLE schema1.test1");
        sql("DROP SCHEMA schema1 RESTRICT");
    }

    @Test
    public void dropSchemaCascade() {
        {
            sql("CREATE SCHEMA schema1");
            sql("CREATE TABLE schema1.test1 (id INT PRIMARY KEY, val INT)");

            sql("DROP SCHEMA schema1 CASCADE");

            assertThrowsSqlException(
                    Sql.STMT_VALIDATION_ERR,
                    "Object 'SCHEMA1' not found",
                    () -> sql("SELECT * FROM schema1.test1")
            );

            assertThrowsSqlException(
                    Sql.STMT_VALIDATION_ERR,
                    "Schema with name 'SCHEMA1' not found",
                    () -> sql("CREATE TABLE schema1.test1 (id INT PRIMARY KEY, val INT)")
            );
        }

        // IF EXISTS

        {
            sql("CREATE SCHEMA schema2");
            sql("CREATE TABLE schema2.test1 (id INT PRIMARY KEY, val INT)");

            sql("DROP SCHEMA IF EXISTS schema2 CASCADE");

            assertThrowsSqlException(
                    Sql.STMT_VALIDATION_ERR,
                    "Object 'SCHEMA2' not found",
                    () -> sql("SELECT * FROM schema2.test1")
            );

            assertThrowsSqlException(
                    Sql.STMT_VALIDATION_ERR,
                    "Schema with name 'SCHEMA2' not found",
                    () -> sql("CREATE TABLE schema2.test1 (id INT PRIMARY KEY, val INT)")
            );
        }
    }

    @Test
    public void schemaQuoted() {
        {
            sql("CREATE SCHEMA IF NOT EXISTS \"Sche ma1\"");
            sql("CREATE TABLE \"Sche ma1\".test1 (id INT PRIMARY KEY, val INT)");
            sql("INSERT INTO \"Sche ma1\".test1 VALUES (1, 1), (2, 2)");

            assertThrowsSqlException(
                    Sql.STMT_VALIDATION_ERR,
                    "Schema with name 'Sche ma1' already exists.",
                    () -> sql("CREATE SCHEMA \"Sche ma1\"")
            );

            assertQuery("SELECT * FROM \"Sche ma1\".test1")
                    .returnRowCount(2)
                    .check();

            sql("DROP SCHEMA \"Sche ma1\" CASCADE");
        }

        {
            sql("CREATE SCHEMA \"Sche ma2\"");
            sql("DROP SCHEMA IF EXISTS \"Sche ma2\"");

            assertThrowsSqlException(
                    Sql.STMT_VALIDATION_ERR,
                    "Schema with name 'Sche ma2' not found",
                    () -> sql("CREATE TABLE \"Sche ma2\".test1 (id INT PRIMARY KEY, val INT)")
            );
        }
    }

    @Test
    public void accessTheSameObject() {
        sql("CREATE SCHEMA s1");
        sql("CREATE TABLE s1.t1 (id INT PRIMARY KEY, val INT)");

        // Works fine
        assertQuery("SELECT count(*) FROM s1.t1")
                .returns(0L)
                .check();

        // Works fine
        assertQuery("SELECT count(*) FROM t1")
                .withDefaultSchema("S1")
                .returns(0L)
                .check();

        // Works fine because we do not access the MISSING schema.
        assertQuery("SELECT count(*) FROM s1.t1")
                .withDefaultSchema("MISSING")
                .returns(0L)
                .check();
    }

    @Test
    public void accessNotExistingObjects() {
        sql("CREATE SCHEMA s1");
        sql("CREATE TABLE s1.t1 (id INT PRIMARY KEY, val INT)");

        // Drop schema
        sql("DROP SCHEMA s1 CASCADE");

        // Object name refers a table
        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                "Object 'T1' not found",
                () -> assertQuery("SELECT count(*) FROM t1").withDefaultSchema("S1").check()
        );

        // Object name refers a schema
        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                "Object 'S1' not found",
                () -> assertQuery("SELECT count(*) FROM S1.t1").check()
        );
        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                "Object 'S1' not found",
                () -> assertQuery("SELECT count(*) FROM S1.t1").withDefaultSchema("S1").check()
        );
    }
}
