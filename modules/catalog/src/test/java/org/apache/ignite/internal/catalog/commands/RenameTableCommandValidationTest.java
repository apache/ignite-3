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

package org.apache.ignite.internal.catalog.commands;

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.pkIndexName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;

import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test suite for validating parameters of {@link RenameTableCommand}s.
 */
public class RenameTableCommandValidationTest extends AbstractCommandValidationTest {
    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void schemaNameMustNotBeNullOrBlank(String name) {
        RenameTableCommandBuilder builder = RenameTableCommand.builder()
                .schemaName(name)
                .tableName("TEST")
                .newTableName("TEST2");

        assertThrows(
                CatalogValidationException.class,
                builder::build,
                "Name of the schema can't be null or blank"
        );
    }

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void tableNameMustNotBeNullOrBlank(String name) {
        RenameTableCommandBuilder builder = RenameTableCommand.builder()
                .schemaName("TEST")
                .tableName(name)
                .newTableName("TEST2");

        assertThrows(
                CatalogValidationException.class,
                builder::build,
                "Name of the table can't be null or blank"
        );
    }

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void newTableNameMustNotBeNullOrBlank(String name) {
        RenameTableCommandBuilder builder = RenameTableCommand.builder()
                .schemaName("TEST")
                .tableName("TEST")
                .newTableName(name);

        assertThrows(
                CatalogValidationException.class,
                builder::build,
                "New table name can't be null or blank"
        );
    }

    @Test
    void exceptionIsThrownIfSchemaDoesNotExist() {
        Catalog catalog = catalogWithDefaultZone();

        CatalogCommand command = RenameTableCommand.builder()
                .schemaName("TEST")
                .tableName("TEST")
                .newTableName("TEST2")
                .build();

        assertThrows(
                CatalogValidationException.class,
                () -> command.get(catalog),
                "Schema with name 'TEST' not found"
        );
    }

    @Test
    void exceptionIsThrownIfTableWithGivenNameNotFound() {
        Catalog catalog = catalogWithDefaultZone();

        CatalogCommand command = RenameTableCommand.builder()
                .schemaName(SCHEMA_NAME)
                .tableName("TEST")
                .newTableName("TEST2")
                .build();

        assertThrows(
                CatalogValidationException.class,
                () -> command.get(catalog),
                "Table with name 'PUBLIC.TEST' not found"
        );
    }

    @Test
    void exceptionIsThrownIfTableWithNewNameExists() {
        Catalog catalog = catalog(
                createTableCommand("TEST"),
                createTableCommand("TEST2")
        );

        CatalogCommand command = RenameTableCommand.builder()
                .schemaName(SCHEMA_NAME)
                .tableName("TEST")
                .newTableName("TEST2")
                .build();

        assertThrows(
                CatalogValidationException.class,
                () -> command.get(catalog),
                "Table with name 'PUBLIC.TEST2' already exists"
        );
    }

    @ParameterizedTest
    @MethodSource("reservedSchemaNames")
    void exceptionIsThrownIfSchemaIsReserved(String schema) {
        RenameTableCommandBuilder builder = RenameTableCommand.builder();

        builder.schemaName(schema)
                .tableName("t");

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Operations with reserved schemas are not allowed"
        );
    }

    @Test
    void exceptionIsThrownIfPkIndexWithNewNameExists() {
        Catalog catalog = catalog(
                createTableCommand("TEST"),
                createTableCommand("TEST3"),
                createIndexCommand("TEST3", pkIndexName("TEST2"))
        );

        CatalogCommand command = RenameTableCommand.builder()
                .schemaName(SCHEMA_NAME)
                .tableName("TEST")
                .newTableName("TEST2")
                .build();

        assertThrows(
                CatalogValidationException.class,
                () -> command.get(catalog),
                String.format("Index with name 'PUBLIC.%s' already exists", pkIndexName("TEST2"))
        );
    }
}
