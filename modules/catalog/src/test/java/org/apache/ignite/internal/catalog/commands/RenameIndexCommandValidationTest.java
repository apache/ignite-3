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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;

import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test suite for validating parameters of {@link RenameIndexCommand}s.
 */
public class RenameIndexCommandValidationTest extends AbstractCommandValidationTest {
    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void schemaNameMustNotBeNullOrBlank(String name) {
        RenameIndexCommandBuilder builder = RenameIndexCommand.builder()
                .schemaName(name)
                .indexName("TEST")
                .newIndexName("TEST2");

        assertThrows(
                CatalogValidationException.class,
                builder::build,
                "Name of the schema can't be null or blank"
        );
    }

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void indexNameMustNotBeNullOrBlank(String name) {
        RenameIndexCommandBuilder builder = RenameIndexCommand.builder()
                .schemaName("TEST")
                .indexName(name)
                .newIndexName("TEST2");

        assertThrows(
                CatalogValidationException.class,
                builder::build,
                "Name of the index can't be null or blank"
        );
    }

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void newIndexNameMustNotBeNullOrBlank(String name) {
        RenameIndexCommandBuilder builder = RenameIndexCommand.builder()
                .schemaName("TEST")
                .indexName("TEST")
                .newIndexName(name);

        assertThrows(
                CatalogValidationException.class,
                builder::build,
                "New index name can't be null or blank"
        );
    }

    @Test
    void exceptionIsThrownIfSchemaDoesNotExist() {
        Catalog catalog = catalogWithDefaultZone();

        CatalogCommand command = RenameIndexCommand.builder()
                .schemaName("TEST")
                .indexName("TEST")
                .newIndexName("TEST2")
                .build();

        assertThrows(
                CatalogValidationException.class,
                () -> command.get(catalog),
                "Schema with name 'TEST' not found"
        );
    }

    @Test
    void exceptionIsThrownIfIndexWithGivenNameNotFound() {
        Catalog catalog = catalogWithDefaultZone();

        CatalogCommand command = RenameIndexCommand.builder()
                .schemaName(SCHEMA_NAME)
                .indexName("TEST")
                .newIndexName("TEST2")
                .build();

        assertThrows(
                CatalogValidationException.class,
                () -> command.get(catalog),
                "Index with name 'PUBLIC.TEST' not found"
        );
    }

    @Test
    void exceptionIsThrownIfIndexWithNewNameExists() {
        Catalog catalog = catalog(
                createTableCommand("TABLE1"),
                createTableCommand("TABLE2"),
                createIndexCommand("TABLE1", "TEST1"),
                createIndexCommand("TABLE2", "TEST2")
        );

        CatalogCommand command = RenameIndexCommand.builder()
                .schemaName(SCHEMA_NAME)
                .indexName("TEST1")
                .newIndexName("TEST2")
                .build();

        assertThrows(
                CatalogValidationException.class,
                () -> command.get(catalog),
                "Index with name 'PUBLIC.TEST2' already exists"
        );
    }

    @Test
    void exceptionIsThrownIfTableWithNewNameExists() {
        Catalog catalog = catalog(
                createTableCommand("TABLE1"),
                createIndexCommand("TABLE1", "TEST")
        );

        CatalogCommand command = RenameIndexCommand.builder()
                .schemaName(SCHEMA_NAME)
                .indexName("TEST")
                .newIndexName("TABLE1")
                .build();

        assertThrows(
                CatalogValidationException.class,
                () -> command.get(catalog),
                "Table with name 'PUBLIC.TABLE1' already exists"
        );
    }

    @ParameterizedTest
    @MethodSource("reservedSchemaNames")
    void exceptionIsThrownIfSchemaIsReserved(String schema) {
        RenameTableCommandBuilder builder = RenameTableCommand.builder();

        builder.schemaName(schema)
                .tableName("t");

        assertThrows(
                CatalogValidationException.class,
                builder::build,
                "Operations with system schemas are not allowed"
        );
    }
}
