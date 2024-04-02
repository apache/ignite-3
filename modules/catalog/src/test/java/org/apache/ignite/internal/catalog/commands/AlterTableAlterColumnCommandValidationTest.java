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

import static org.apache.ignite.internal.catalog.CatalogTestUtils.initializeColumnWithDefaults;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify validation of {@link AlterTableAlterColumnCommand}.
 */
@SuppressWarnings("ThrowableNotThrown")
public class AlterTableAlterColumnCommandValidationTest extends AbstractCommandValidationTest {
    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void schemaNameMustNotBeNullOrBlank(String name) {
        AlterTableAlterColumnCommandBuilder builder = AlterTableAlterColumnCommand.builder();

        builder.tableName("TEST")
                .columnName("TEST")
                .schemaName(name)
                .nullable(true);

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Name of the schema can't be null or blank"
        );
    }

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void tableNameMustNotBeNullOrBlank(String name) {
        AlterTableAlterColumnCommandBuilder builder = AlterTableAlterColumnCommand.builder();

        builder.schemaName("TEST")
                .columnName("TEST")
                .tableName(name)
                .nullable(true);

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Name of the table can't be null or blank"
        );
    }

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void columnNameMustNotBeNullOrBlank(String name) {
        AlterTableAlterColumnCommandBuilder builder = AlterTableAlterColumnCommand.builder();

        builder.schemaName("TEST")
                .tableName("TEST")
                .columnName(name)
                .nullable(true);

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Name of the column can't be null or blank"
        );
    }

    @Test
    void exceptionIsThrownIfSchemaNotExists() {
        AlterTableAlterColumnCommandBuilder builder = AlterTableAlterColumnCommand.builder();

        Catalog catalog = emptyCatalog();

        CatalogCommand command = builder
                .schemaName(SCHEMA_NAME + "_UNK")
                .tableName("TEST")
                .columnName("TEST")
                .nullable(true)
                .build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                "Schema with name 'PUBLIC_UNK' not found"
        );
    }

    @Test
    void exceptionIsThrownIfTableWithGivenNameNotFound() {
        AlterTableAlterColumnCommandBuilder builder = AlterTableAlterColumnCommand.builder();

        Catalog catalog = emptyCatalog();

        CatalogCommand command = builder
                .schemaName(SCHEMA_NAME)
                .tableName("TEST")
                .columnName("TEST")
                .nullable(true)
                .build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                "Table with name 'PUBLIC.TEST' not found"
        );
    }

    @Test
    void exceptionIsThrownIfColumnWithGivenNameNotFound() {
        AlterTableAlterColumnCommandBuilder builder = AlterTableAlterColumnCommand.builder();

        String tableName = "TEST";
        Catalog catalog = catalogWithTable(tableName);

        CatalogCommand command = builder
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columnName("TEST")
                .nullable(true)
                .build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                "Column with name 'TEST' not found in table 'PUBLIC.TEST'"
        );
    }

    @Test
    void typeOfPkColumnCannotBeChanged() {
        String tableName = "TEST";
        AlterTableAlterColumnCommandBuilder builder = AlterTableAlterColumnCommand.builder();

        Catalog catalog = catalogWithTable(tableName);

        CatalogCommand command = builder
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columnName("ID")
                .type(ColumnType.INT64)
                .build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                "Changing the type of key column is not allowed"
        );
    }

    @Test
    void precisionOfPkColumnCannotBeChanged() {
        String tableName = "TEST";
        String columnName = "ID";
        Catalog catalog = catalogWithTable(builder -> builder
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columns(List.of(
                        ColumnParams.builder().name(columnName).type(ColumnType.DECIMAL).precision(10).scale(0).build())
                )
                .primaryKey(primaryKey(columnName))
        );

        AlterTableAlterColumnCommandBuilder builder = AlterTableAlterColumnCommand.builder();

        CatalogCommand command = builder
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columnName(columnName)
                .precision(16)
                .build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                "Changing the precision of key column is not allowed"
        );
    }

    @Test
    void scaleOfPkColumnCannotBeChanged() {
        String tableName = "TEST";
        String columnName = "ID";
        Catalog catalog = catalogWithTable(builder -> builder
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columns(List.of(
                        ColumnParams.builder()
                                .name(columnName)
                                .type(ColumnType.DECIMAL)
                                .precision(10)
                                .scale(2)
                                .build())
                )
                .primaryKey(primaryKey(columnName))
        );

        AlterTableAlterColumnCommandBuilder builder = AlterTableAlterColumnCommand.builder();

        CatalogCommand command = builder
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columnName(columnName)
                .scale(6)
                .build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                "Changing the scale of key column is not allowed"
        );
    }

    @Test
    void notNullConstraintOfPkColumnCannotBeDropped() {
        String tableName = "TEST";
        String columnName = "ID";
        Catalog catalog = catalogWithTable(builder -> builder
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columns(List.of(
                        ColumnParams.builder()
                                .name(columnName)
                                .type(ColumnType.INT64)
                                .build())
                )
                .primaryKey(primaryKey(columnName))
        );

        AlterTableAlterColumnCommandBuilder builder = AlterTableAlterColumnCommand.builder();

        CatalogCommand command = builder
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columnName(columnName)
                .nullable(true)
                .build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                "Dropping NOT NULL constraint on key column is not allowed"
        );
    }

    @ParameterizedTest
    @MethodSource("invalidTypeConversionPairs")
    void invalidTypeConversionIsNotAllowed(ColumnType from, ColumnType to) {
        String tableName = "TEST";
        String columnName = "VAL";
        Catalog catalog = catalogWithTable(builder -> builder
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columns(List.of(
                        ColumnParams.builder()
                                .name("ID")
                                .type(ColumnType.INT32)
                                .build(),
                        initializeColumnWithDefaults(from, ColumnParams.builder()
                                .name(columnName)
                                .type(from))
                                .build())
                )
                .primaryKey(primaryKey("ID"))
        );

        AlterTableAlterColumnCommandBuilder builder = AlterTableAlterColumnCommand.builder();

        CatalogCommand command = builder
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columnName(columnName)
                .type(to)
                .build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                format("Changing the type from {} to {} is not allowed", from, to)
        );
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.EXCLUDE, value = ColumnType.class, names = {"DECIMAL", "NULL"})
    void precisionCannotBeChangedIfTypeIsNotDecimal(ColumnType type) {
        String tableName = "TEST";
        String columnName = "VAL";
        Catalog catalog = catalogWithTable(builder -> builder
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columns(List.of(
                        initializeColumnWithDefaults(ColumnType.INT64, ColumnParams.builder()
                                .name("ID")
                                .type(ColumnType.INT64))
                                .build(),
                        initializeColumnWithDefaults(type, ColumnParams.builder()
                                .name(columnName)
                                .type(type))
                                .build())
                )
                .primaryKey(primaryKey("ID"))
        );

        AlterTableAlterColumnCommandBuilder builder = AlterTableAlterColumnCommand.builder();

        CatalogCommand command = builder
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columnName(columnName)
                .precision(2)
                .build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                format("the precision for column of type '{}' is not allowed", type)
        );
    }

    @Test
    void precisionCannotBeDecreased() {
        String tableName = "TEST";
        String columnName = "VAL";
        Catalog catalog = catalogWithTable(builder -> builder
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columns(List.of(
                        ColumnParams.builder()
                                .name("ID")
                                .type(ColumnType.INT64)
                                .build(),
                        ColumnParams.builder()
                                .name(columnName)
                                .type(ColumnType.DECIMAL)
                                .scale(0)
                                .precision(10)
                                .build())
                )
                .primaryKey(primaryKey("ID"))
        );

        AlterTableAlterColumnCommandBuilder builder = AlterTableAlterColumnCommand.builder();

        CatalogCommand command = builder
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columnName(columnName)
                .precision(2)
                .build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                "Changing the precision for column of type"
        );
    }

    @Test
    void scaleCannotBeChanged() {
        String tableName = "TEST";
        String columnName = "VAL";
        Catalog catalog = catalogWithTable(builder -> builder
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columns(List.of(
                        ColumnParams.builder()
                                .name("ID")
                                .type(ColumnType.INT64)
                                .build(),
                        ColumnParams.builder()
                                .name(columnName)
                                .type(ColumnType.DECIMAL)
                                .precision(10)
                                .scale(6)
                                .build())
                )
                .primaryKey(primaryKey("ID"))
        );

        AlterTableAlterColumnCommandBuilder builder = AlterTableAlterColumnCommand.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columnName(columnName);

        assertThrowsWithCause(
                () -> builder.scale(2).build().get(catalog),
                CatalogValidationException.class,
                "Changing the scale for column of type"
        );

        assertThrowsWithCause(
                () -> builder.scale(10).build().get(catalog),
                CatalogValidationException.class,
                "Changing the scale for column of type"
        );
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.EXCLUDE, value = ColumnType.class, names = {"STRING", "BYTE_ARRAY", "NULL"})
    void lengthCannotBeChangedForNonVariableTypes(ColumnType type) {
        String tableName = "TEST";
        String columnName = "VAL";
        Catalog catalog = catalogWithTable(builder -> builder
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columns(List.of(
                        ColumnParams.builder()
                                .name("ID")
                                .type(ColumnType.INT64)
                                .build(),
                        initializeColumnWithDefaults(type, ColumnParams.builder()
                                .name(columnName)
                                .type(type))
                                .build())
                )
                .primaryKey(primaryKey("ID"))
        );

        AlterTableAlterColumnCommandBuilder builder = AlterTableAlterColumnCommand.builder();

        CatalogCommand command = builder
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columnName(columnName)
                .length(2)
                .build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                format("Changing the length for column of type '{}' is not allowed", type)
        );
    }

    @Test
    void lengthCannotBeDecreased() {
        String tableName = "TEST";
        String columnName = "VAL";
        Catalog catalog = catalogWithTable(builder -> builder
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columns(List.of(
                        ColumnParams.builder()
                                .name("ID")
                                .type(ColumnType.INT64)
                                .build(),
                        ColumnParams.builder()
                                .name(columnName)
                                .type(ColumnType.STRING)
                                .length(10)
                                .build())
                )
                .primaryKey(primaryKey("ID"))
        );

        AlterTableAlterColumnCommandBuilder builder = AlterTableAlterColumnCommand.builder();

        CatalogCommand command = builder
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columnName(columnName)
                .length(2)
                .build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                "Changing the length for column of type"
        );
    }

    @Test
    void notNullConstraintCannotBeAddedToNullableColumn() {
        String tableName = "TEST";
        String columnName = "VAL";
        Catalog catalog = catalogWithTable(builder -> builder
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columns(List.of(
                        ColumnParams.builder()
                                .name("ID")
                                .type(ColumnType.INT64)
                                .build(),
                        ColumnParams.builder()
                                .name(columnName)
                                .type(ColumnType.INT64)
                                .nullable(true)
                                .build())
                )
                .primaryKey(primaryKey("ID"))
        );

        AlterTableAlterColumnCommandBuilder builder = AlterTableAlterColumnCommand.builder();

        CatalogCommand command = builder
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columnName(columnName)
                .nullable(false)
                .build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                "Adding NOT NULL constraint is not allowed"
        );
    }

    @ParameterizedTest
    @MethodSource("reservedSchemaNames")
    void exceptionIsThrownIfSchemaIsReserved(String schema) {
        AlterTableAlterColumnCommandBuilder builder = AlterTableAlterColumnCommand.builder();

        builder.schemaName(schema)
                .tableName("t");

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Operations with reserved schemas are not allowed"
        );
    }

    private static Stream<Arguments> invalidTypeConversionPairs() {
        List<Arguments> arguments = new ArrayList<>();
        for (ColumnType from : ColumnType.values()) {
            for (ColumnType to : ColumnType.values()) {
                if (from != to && !CatalogUtils.isSupportedColumnTypeChange(from, to) && from != ColumnType.NULL) {
                    arguments.add(Arguments.of(from, to));
                }
            }
        }

        return arguments.stream();
    }
}
