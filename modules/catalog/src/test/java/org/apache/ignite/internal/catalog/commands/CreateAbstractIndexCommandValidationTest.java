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
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify validation of {@link AbstractCreateIndexCommand}.
 */
@SuppressWarnings({"ThrowableNotThrown", "rawtypes", "unchecked"})
public abstract class CreateAbstractIndexCommandValidationTest extends AbstractCommandValidationTest {
    protected static final String INDEX_NAME = "IDX";

    protected abstract <T extends AbstractCreateIndexCommandBuilder<T>> T prefilledBuilder();

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void schemaNameMustNotBeNullOrBlank(String name) {
        AbstractCreateIndexCommandBuilder builder = prefilledBuilder();

        builder.schemaName(name);

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Name of the schema can't be null or blank"
        );
    }

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void indexNameMustNotBeNullOrBlank(String name) {
        AbstractCreateIndexCommandBuilder builder = prefilledBuilder();

        builder.indexName(name);

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Name of the index can't be null or blank"
        );
    }

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void tableNameMustNotBeNullOrBlank(String name) {
        AbstractCreateIndexCommandBuilder builder = prefilledBuilder();

        builder.tableName(name);

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Name of the table can't be null or blank"
        );
    }

    @ParameterizedTest(name = "[{index}] {argumentsWithNames}")
    @MethodSource("nullAndEmptyLists")
    void indexShouldHaveAtLeastOneColumn(List<String> columns) {
        AbstractCreateIndexCommandBuilder builder = prefilledBuilder();

        builder.columns(columns);

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Columns not specified"
        );
    }

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void columnNameMustNotBeNullOrBlank(String name) {
        AbstractCreateIndexCommandBuilder builder = prefilledBuilder();

        List<String> names = new ArrayList<>();
        names.add(name);

        builder.columns(names);

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Name of the column can't be null or blank"
        );
    }

    @Test
    void columnShouldNotHaveDuplicates() {
        AbstractCreateIndexCommandBuilder builder = prefilledBuilder();

        builder.columns(List.of("C", "C"));

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Column with name 'C' specified more than once"
        );
    }

    @Test
    void exceptionIsThrownIfSchemaNotExists() {
        Catalog catalog = catalogWithDefaultZone();

        CatalogCommand command = prefilledBuilder().schemaName(SCHEMA_NAME + "_UNK").build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                "Schema with name 'PUBLIC_UNK' not found"
        );
    }

    @Test
    void exceptionIsThrownIfTableNotExists() {
        Catalog catalog = catalogWithDefaultZone();

        CatalogCommand command = prefilledBuilder().build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                "Table with name 'PUBLIC.TEST' not found"
        );
    }

    @Test
    void exceptionIsThrownIfTableWithGivenNameAlreadyExists() {
        Catalog catalog = catalogWithTable("TEST");

        CatalogCommand command = prefilledBuilder()
                .tableName("TEST")
                .indexName("TEST")
                .build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                "Table with name 'PUBLIC.TEST' already exists"
        );
    }

    @Test
    void exceptionIsThrownIfIndexWithGivenNameAlreadyExists() {
        Catalog catalog = catalogWithIndex("IDX");

        CatalogCommand command = prefilledBuilder().indexName("IDX").build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                "Index with name 'PUBLIC.IDX' already exists"
        );
    }

    @Test
    void exceptionIsThrownIfColumnWithGivenNameNotExists() {
        String tableName = "TEST";

        Catalog catalog = catalogWithTable(tableName);

        CatalogCommand command = prefilledBuilder().columns(List.of("UNK")).build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                "Column with name 'UNK' not found in table 'PUBLIC.TEST'"
        );
    }

    @Test
    void exceptionIsThrownIfIndexIsUniqueAndNotAllColocationColumnsCovered() {
        String tableName = "TEST";

        ColumnParams c1 = ColumnParams.builder().name("C1").type(INT32).build();
        ColumnParams c2 = ColumnParams.builder().name("C2").type(INT32).build();

        Catalog catalog = catalogWithTable(builder -> builder
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .columns(List.of(c1, c2))
                .primaryKey(primaryKey(c1.name(), c2.name()))
        );

        CatalogCommand command = prefilledBuilder()
                .unique(true)
                .columns(List.of(c1.name()))
                .build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                "Unique index must include all colocation columns"
        );
    }

    @Test
    void noExceptionIsThrownIfStoppingIndexWithGivenNameAlreadyExists() {
        String indexName = "IDX";

        Catalog catalog = catalogWithIndex(indexName);

        CatalogCommand createIndexCommand = prefilledBuilder().indexName(indexName).build();

        assertThrows(CatalogValidationException.class, () -> applyCommandsToCatalog(catalog, createIndexCommand));

        Catalog newCatalog = transitionIndexToStoppingState(catalog, indexName);

        assertDoesNotThrow(() -> applyCommandsToCatalog(newCatalog, createIndexCommand));
    }
}
