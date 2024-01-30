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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.sql.ColumnType.INT32;

import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify validation of {@link CreateTableCommand}.
 */
@SuppressWarnings({"DataFlowIssue", "ThrowableNotThrown"})
public class CreateTableCommandValidationTest extends AbstractCommandValidationTest {
    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void schemaNameMustNotBeNullOrBlank(String name) {
        CreateTableCommandBuilder builder = CreateTableCommand.builder();

        builder = fillProperties(builder);

        builder.schemaName(name);

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Name of the schema can't be null or blank"
        );
    }

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void tableNameMustNotBeNullOrBlank(String name) {
        CreateTableCommandBuilder builder = CreateTableCommand.builder();

        builder = fillProperties(builder);

        builder.tableName(name);

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Name of the table can't be null or blank"
        );
    }

    @ParameterizedTest(name = "[{index}] {argumentsWithNames}")
    @MethodSource("nullAndEmptyLists")
    void tableShouldHaveAtLeastOneColumn(List<ColumnParams> columns) {
        CreateTableCommandBuilder builder = CreateTableCommand.builder();

        builder = fillProperties(builder);

        builder.columns(columns);

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Table should have at least one column"
        );
    }

    @Test
    void columnShouldNotHaveDuplicates() {
        CreateTableCommandBuilder builder = CreateTableCommand.builder();

        ColumnParams column = ColumnParams.builder()
                .name("C")
                .type(INT32)
                .build();

        builder = fillProperties(builder).columns(List.of(column, column));

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Column with name 'C' specified more than once"
        );
    }

    @ParameterizedTest(name = "[{index}] {argumentsWithNames}")
    @MethodSource("nullAndEmptyLists")
    void tableShouldHaveAtLeastOnePrimaryKeyColumn(List<String> columns) {
        CreateTableCommandBuilder builder = CreateTableCommand.builder();

        builder = fillProperties(builder);

        builder.primaryKeyColumns(columns);

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Table should have primary key"
        );
    }

    @Test
    void pkColumnShouldNotHaveDuplicates() {
        CreateTableCommandBuilder builder = CreateTableCommand.builder();

        builder = fillProperties(builder)
                .primaryKeyColumns(List.of("C", "C"));

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "PK column 'C' specified more that once"
        );
    }

    @Test
    void pkColumnShouldBePresentedInColumnsList() {
        CreateTableCommandBuilder builder = CreateTableCommand.builder();

        builder = fillProperties(builder)
                .primaryKeyColumns(List.of("foo"));

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "PK column 'foo' is not part of table"
        );
    }

    @Test
    void colocationColumnsCouldNotBeEmpty() {
        CreateTableCommandBuilder builder = CreateTableCommand.builder();

        builder = fillProperties(builder)
                .colocationColumns(List.of());

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Colocation columns could not be empty"
        );
    }

    @Test
    void colocationColumnShouldNotHaveDuplicates() {
        CreateTableCommandBuilder builder = CreateTableCommand.builder();

        builder = fillProperties(builder)
                .colocationColumns(List.of("C", "C"));

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Colocation column 'C' specified more that once"
        );
    }

    @Test
    void colocationColumnShouldBePresentedInColumnsList() {
        CreateTableCommandBuilder builder = CreateTableCommand.builder();

        ColumnParams c1 = ColumnParams.builder()
                .name("C1")
                .type(INT32)
                .build();

        ColumnParams c2 = ColumnParams.builder()
                .name("C2")
                .type(INT32)
                .build();

        builder = fillProperties(builder)
                .columns(List.of(c1, c2))
                .primaryKeyColumns(List.of("C1"))
                .colocationColumns(List.of("C2"));

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Colocation column 'C2' is not part of PK"
        );
    }

    private static CreateTableCommandBuilder fillProperties(CreateTableCommandBuilder builder) {
        return builder
                .schemaName(SCHEMA_NAME)
                .tableName("TEST")
                .zone(ZONE_NAME)
                .columns(List.of(
                        ColumnParams.builder()
                                .name("C")
                                .type(INT32)
                                .build()
                ))
                .primaryKeyColumns(List.of("C"))
                .colocationColumns(List.of("C"));
    }

    @Test
    void exceptionIsThrownIfSchemaNotExists() {
        CreateTableCommandBuilder builder = CreateTableCommand.builder();

        Catalog catalog = emptyCatalog();

        CatalogCommand command = fillProperties(builder).schemaName(SCHEMA_NAME + "_UNK").build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                "Schema with name 'PUBLIC_UNK' not found"
        );
    }

    @ParameterizedTest
    @MethodSource("reservedSchemaNames")
    void exceptionIsThrownIfSchemaIsReserved(String schema) {
        CreateTableCommandBuilder builder = CreateTableCommand.builder();

        builder = fillProperties(builder).schemaName(schema);

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Operations with reserved schemas are not allowed"
        );
    }

    @Test
    void exceptionIsThrownIfZoneNotExists() {
        CreateTableCommandBuilder builder = CreateTableCommand.builder();

        Catalog catalog = emptyCatalog();

        CatalogCommand command = fillProperties(builder).zone(ZONE_NAME + "_UNK").build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                "Distribution zone with name 'Default_UNK' not found"
        );
    }

    @Test
    void exceptionIsThrownIfTableWithGivenNameAlreadyExists() {
        CreateTableCommandBuilder builder = CreateTableCommand.builder();

        Catalog catalog = catalogWithTable("TEST");

        CatalogCommand command = fillProperties(builder).tableName("TEST").build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                "Table with name 'PUBLIC.TEST' already exists"
        );
    }

    @Test
    void exceptionIsThrownIfIndexWithGivenNameAlreadyExists() {
        CreateTableCommandBuilder builder = CreateTableCommand.builder();

        Catalog catalog = catalogWithIndex("TEST_IDX");

        CatalogCommand command = fillProperties(builder).tableName("TEST_IDX").build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                "Index with name 'PUBLIC.TEST_IDX' already exists"
        );
    }

    @Test
    void exceptionIsThrownIfTableWithNameSimilarToAutogeneratedPkNameAlreadyExists() {
        CreateTableCommandBuilder builder = CreateTableCommand.builder();

        Catalog catalog = catalogWithTable("TEST_PK");

        CatalogCommand command = fillProperties(builder).tableName("TEST").build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                "Table with name 'PUBLIC.TEST_PK' already exists"
        );
    }

    @Test
    void exceptionIsThrownIfIndexWithNameSimilarToAutogeneratedPkNameAlreadyExists() {
        CreateTableCommandBuilder builder = CreateTableCommand.builder();

        Catalog catalog = catalogWithIndex("FOO_PK");

        CatalogCommand command = fillProperties(builder).tableName("FOO").build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                "Index with name 'PUBLIC.FOO_PK' already exists"
        );
    }
}
