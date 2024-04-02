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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify validation of {@link AlterTableDropColumnCommand}.
 */
@SuppressWarnings({"ThrowableNotThrown"})
public class AlterTableDropColumnCommandValidationTest extends AbstractCommandValidationTest {
    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void schemaNameMustNotBeNullOrBlank(String name) {
        AlterTableDropColumnCommandBuilder builder = AlterTableDropColumnCommand.builder();

        builder = fillProperties(builder);

        builder.schemaName(name);

        assertThrows(
                CatalogValidationException.class,
                builder::build,
                "Name of the schema can't be null or blank"
        );
    }

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void tableNameMustNotBeNullOrBlank(String name) {
        AlterTableDropColumnCommandBuilder builder = AlterTableDropColumnCommand.builder();

        builder = fillProperties(builder);

        builder.tableName(name);

        assertThrows(
                CatalogValidationException.class,
                builder::build,
                "Name of the table can't be null or blank"
        );
    }

    @ParameterizedTest(name = "[{index}] {argumentsWithNames}")
    @MethodSource("nullAndEmptySets")
    void commandShouldHaveAtLeastOneColumn(Set<String> columns) {
        AlterTableDropColumnCommandBuilder builder = AlterTableDropColumnCommand.builder();

        builder = fillProperties(builder);

        builder.columns(columns);

        assertThrows(
                CatalogValidationException.class,
                builder::build,
                "Columns not specified"
        );
    }

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void columnNameMustNotBeNullOrBlank(String name) {
        AlterTableDropColumnCommandBuilder builder = AlterTableDropColumnCommand.builder();

        builder = fillProperties(builder);

        Set<String> columnNames = new HashSet<>();
        columnNames.add(name);

        builder.columns(columnNames);

        assertThrows(
                CatalogValidationException.class,
                builder::build,
                "Name of the column can't be null or blank"
        );
    }

    private static AlterTableDropColumnCommandBuilder fillProperties(AlterTableDropColumnCommandBuilder builder) {
        return builder
                .schemaName(SCHEMA_NAME)
                .tableName("TEST")
                .columns(Set.of("C"));
    }

    @Test
    void exceptionIsThrownIfSchemaNotExists() {
        AlterTableDropColumnCommandBuilder builder = AlterTableDropColumnCommand.builder();

        Catalog catalog = emptyCatalog();

        CatalogCommand command = fillProperties(builder).schemaName(SCHEMA_NAME + "_UNK").build();

        assertThrows(
                CatalogValidationException.class,
                () -> command.get(catalog),
                "Schema with name 'PUBLIC_UNK' not found"
        );
    }

    @Test
    void exceptionIsThrownIfTableNotExists() {
        AlterTableDropColumnCommandBuilder builder = AlterTableDropColumnCommand.builder();

        Catalog catalog = emptyCatalog();

        CatalogCommand command = fillProperties(builder).tableName("TEST").build();

        assertThrows(
                CatalogValidationException.class,
                () -> command.get(catalog),
                "Table with name 'PUBLIC.TEST' not found"
        );
    }

    @Test
    void exceptionIsThrownIfColumnWithGivenNameNotExists() {
        String tableName = "TEST";
        String columnName = "TEST";
        Catalog catalog = catalogWithTable(builder -> builder
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columns(List.of(ColumnParams.builder().name(columnName).type(INT32).build()))
                .primaryKey(primaryKey(columnName))
        );

        AlterTableDropColumnCommandBuilder builder = AlterTableDropColumnCommand.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columns(Set.of(columnName + "_UNK"));

        assertThrows(
                CatalogValidationException.class,
                () -> builder.build().get(catalog),
                "Column with name 'TEST_UNK' not found in table 'PUBLIC.TEST'"
        );
    }

    @Test
    void exceptionIsThrownIfColumnBelongsToPrimaryKey() {
        String tableName = "TEST";
        String columnName1 = "C1";
        String columnName2 = "C2";
        Catalog catalog = catalogWithTable(builder -> builder
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columns(List.of(
                        ColumnParams.builder().name(columnName1).type(INT32).build(),
                        ColumnParams.builder().name(columnName2).type(INT32).build()
                ))
                .primaryKey(primaryKey(columnName1, columnName2))
        );

        AlterTableDropColumnCommandBuilder builder = AlterTableDropColumnCommand.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columns(Set.of(columnName2));

        assertThrows(
                CatalogValidationException.class,
                () -> builder.build().get(catalog),
                "Deleting column `C2` belonging to primary key is not allowed"
        );
    }

    @Test
    void exceptionIsThrownIfColumnBelongsToIndex() {
        Catalog catalog = catalogWithIndex("TEST_IDX");

        AlterTableDropColumnCommandBuilder builder = AlterTableDropColumnCommand.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(Set.of("VAL"));

        assertThrows(
                CatalogValidationException.class,
                () -> builder.build().get(catalog),
                "Deleting column 'VAL' used by index(es) [TEST_IDX], it is not allowed"
        );
    }

    @Test
    void rightExceptionIsThrownIfSameColumnNameBelongsToIndexesForDifferentTables() {
        Catalog catalog = catalog(
                createTableCommand(TABLE_NAME),
                createIndexCommand(TABLE_NAME, "TEST_IDX"),
                createTableCommand(TABLE_NAME + "_1"),
                createIndexCommand(TABLE_NAME + "_1", "TEST_IDX" + "_1")
        );

        Set<String> indexes = catalog.indexes().stream()
                .filter(index -> ((CatalogHashIndexDescriptor) index).columns().contains("VAL"))
                .map(CatalogObjectDescriptor::name)
                .collect(Collectors.toSet());

        assertEquals(2, indexes.size(), "should be 2 indexes with column `VAL`");

        AlterTableDropColumnCommandBuilder builder = AlterTableDropColumnCommand.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(Set.of("VAL"));

        assertThrows(
                CatalogValidationException.class,
                () -> builder.build().get(catalog),
                "Deleting column 'VAL' used by index(es) [TEST_IDX], it is not allowed"
        );
    }

    @ParameterizedTest
    @MethodSource("reservedSchemaNames")
    void exceptionIsThrownIfSchemaIsReserved(String schema) {
        AlterTableDropColumnCommandBuilder builder = AlterTableDropColumnCommand.builder();

        builder.schemaName(schema)
                .tableName("t");

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Operations with reserved schemas are not allowed"
        );
    }

    @Test
    void noExceptionIsThrownIfColumnBelongsToStoppingIndex() {
        String indexName = "TEST_IDX";

        Catalog catalog = catalogWithIndex(indexName);

        CatalogCommand dropColumnCommand = AlterTableDropColumnCommand.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(Set.of("VAL"))
                .build();

        Assertions.assertThrows(CatalogValidationException.class, () -> applyCommandsToCatalog(catalog, dropColumnCommand));

        Catalog newCatalog = transitionIndexToStoppingState(catalog, indexName);

        assertDoesNotThrow(() -> applyCommandsToCatalog(newCatalog, dropColumnCommand));
    }
}
