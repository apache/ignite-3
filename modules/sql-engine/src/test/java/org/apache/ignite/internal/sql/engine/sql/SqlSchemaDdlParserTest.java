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

package org.apache.ignite.internal.sql.engine.sql;

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.junit.jupiter.api.Test;

/**
 * Tests to verify parsing of the DDL "SCHEMA" commands.
 */
public class SqlSchemaDdlParserTest extends AbstractParserTest {
    @Test
    public void createSchema() {
        IgniteSqlCreateSchema createSchema = parseCreateSchema("create schema test_schema");

        assertFalse(createSchema.ifNotExists());

        expectUnparsed(createSchema, "CREATE SCHEMA \"TEST_SCHEMA\"");
    }

    @Test
    public void createSchemaInvalidSyntax() {
        assertThrowsSqlException(Sql.STMT_PARSE_ERR, "Encountered \"cascade\"", () -> parse("create schema test cascade"));
    }

    @Test
    public void createSchemaIfNotExists() {
        IgniteSqlCreateSchema createSchema = parseCreateSchema("create schema if not exists test_schema");

        assertTrue(createSchema.ifNotExists());

        expectUnparsed(createSchema, "CREATE SCHEMA IF NOT EXISTS \"TEST_SCHEMA\"");
    }

    @Test
    public void dropSchema() {
        IgniteSqlDropSchema dropSchema = parseDropSchema("drop schema test_schema");

        assertFalse(dropSchema.ifExists());
        assertSame(IgniteSqlDropSchemaBehavior.IMPLICIT_RESTRICT, dropSchema.behavior());

        expectUnparsed(dropSchema, "DROP SCHEMA \"TEST_SCHEMA\"");
    }

    @Test
    public void dropSchemaIfExists() {
        IgniteSqlDropSchema dropSchema = parseDropSchema("drop schema if exists test_schema");

        assertTrue(dropSchema.ifExists());
        assertSame(IgniteSqlDropSchemaBehavior.IMPLICIT_RESTRICT, dropSchema.behavior());

        expectUnparsed(dropSchema, "DROP SCHEMA IF EXISTS \"TEST_SCHEMA\"");
    }

    @Test
    public void dropSchemaBehavior() {
        // CASCADE
        {
            IgniteSqlDropSchema dropSchema = parseDropSchema("drop schema test_schema cascade");

            assertFalse(dropSchema.ifExists());
            assertSame(IgniteSqlDropSchemaBehavior.CASCADE, dropSchema.behavior());

            expectUnparsed(dropSchema, "DROP SCHEMA \"TEST_SCHEMA\" CASCADE");
        }

        // RESTRICT
        {
            IgniteSqlDropSchema dropSchema = parseDropSchema("drop schema test_schema restrict");

            assertFalse(dropSchema.ifExists());
            assertSame(IgniteSqlDropSchemaBehavior.RESTRICT, dropSchema.behavior());

            expectUnparsed(dropSchema, "DROP SCHEMA \"TEST_SCHEMA\" RESTRICT");
        }
    }

    @Test
    public void dropSchemaIfExistsAndBehavior() {
        // CASCADE
        {
            IgniteSqlDropSchema dropSchema = parseDropSchema("drop schema if exists test_schema cascade");

            assertTrue(dropSchema.ifExists());
            assertSame(IgniteSqlDropSchemaBehavior.CASCADE, dropSchema.behavior());

            expectUnparsed(dropSchema, "DROP SCHEMA IF EXISTS \"TEST_SCHEMA\" CASCADE");
        }

        // RESTRICT
        {
            IgniteSqlDropSchema dropSchema = parseDropSchema("drop schema if exists test_schema restrict");

            assertTrue(dropSchema.ifExists());
            assertSame(IgniteSqlDropSchemaBehavior.RESTRICT, dropSchema.behavior());

            expectUnparsed(dropSchema, "DROP SCHEMA IF EXISTS \"TEST_SCHEMA\" RESTRICT");
        }
    }

    @Test
    public void dropSchemaInvalidSyntax() {
        assertThrowsSqlException(Sql.STMT_PARSE_ERR, "Encountered \"cascade1\"", () -> parse("drop schema test cascade1"));
    }

    private static IgniteSqlCreateSchema parseCreateSchema(String stmt) {
        SqlNode node = parse(stmt);

        return assertInstanceOf(IgniteSqlCreateSchema.class, node);
    }

    private static IgniteSqlDropSchema parseDropSchema(String stmt) {
        SqlNode node = parse(stmt);

        return assertInstanceOf(IgniteSqlDropSchema.class, node);
    }
}
