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
 * Tests to verify validation of {@link AlterTableAddColumnCommand}.
 */
@SuppressWarnings({"DataFlowIssue", "ThrowableNotThrown"})
public class AlterTableAddColumnCommandValidationTest extends AbstractCommandValidationTest {
    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void schemaNameMustNotBeNullOrBlank(String name) {
        AlterTableAddColumnCommandBuilder builder = AlterTableAddColumnCommand.builder();

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
        AlterTableAddColumnCommandBuilder builder = AlterTableAddColumnCommand.builder();

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
    void commandShouldHaveAtLeastOneColumn(List<ColumnParams> columns) {
        AlterTableAddColumnCommandBuilder builder = AlterTableAddColumnCommand.builder();

        builder = fillProperties(builder);

        builder.columns(columns);

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Columns not specified"
        );
    }

    @Test
    void columnShouldNotHaveDuplicates() {
        AlterTableAddColumnCommandBuilder builder = AlterTableAddColumnCommand.builder();

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

    private static AlterTableAddColumnCommandBuilder fillProperties(AlterTableAddColumnCommandBuilder builder) {
        return builder
                .schemaName(SCHEMA_NAME)
                .tableName("TEST")
                .columns(List.of(
                        ColumnParams.builder()
                                .name("NEW_C")
                                .type(INT32)
                                .build()
                ));
    }

    @Test
    void exceptionIsThrownIfSchemaNotExists() {
        AlterTableAddColumnCommandBuilder builder = AlterTableAddColumnCommand.builder();

        Catalog catalog = catalogWithDefaultZone();

        CatalogCommand command = fillProperties(builder).schemaName(SCHEMA_NAME + "_UNK").build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                "Schema with name 'PUBLIC_UNK' not found"
        );
    }

    @Test
    void exceptionIsThrownIfTableNotExists() {
        AlterTableAddColumnCommandBuilder builder = AlterTableAddColumnCommand.builder();

        Catalog catalog = catalogWithDefaultZone();

        CatalogCommand command = fillProperties(builder).tableName("TEST").build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                "Table with name 'PUBLIC.TEST' not found"
        );
    }

    @Test
    void exceptionIsThrownIfColumnWithGivenNameAlreadyExists() {
        String tableName = "TEST";
        String columnName = "TEST";
        ColumnParams columnParams = ColumnParams.builder().name(columnName).type(INT32).build();
        Catalog catalog = catalogWithTable(builder -> builder
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columns(List.of(columnParams))
                .primaryKey(primaryKey(columnName))
        );

        AlterTableAddColumnCommandBuilder builder = AlterTableAddColumnCommand.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columns(List.of(columnParams));

        assertThrowsWithCause(
                () -> builder.build().get(catalog),
                CatalogValidationException.class,
                "Column with name 'TEST' already exists"
        );
    }

    @ParameterizedTest
    @MethodSource("reservedSchemaNames")
    void exceptionIsThrownIfSchemaIsReserved(String schema) {
        AlterTableAddColumnCommandBuilder builder = AlterTableAddColumnCommand.builder();

        builder.schemaName(schema)
                .tableName("t");

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Operations with reserved schemas are not allowed"
        );
    }
}
