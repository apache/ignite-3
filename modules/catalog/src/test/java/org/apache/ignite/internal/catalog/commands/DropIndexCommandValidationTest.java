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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;

import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.IndexNotFoundValidationException;
import org.apache.ignite.internal.catalog.UpdateContext;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify validation of {@link DropIndexCommand}.
 */
@SuppressWarnings("ThrowableNotThrown")
public class DropIndexCommandValidationTest extends AbstractCommandValidationTest {

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void schemaNameMustNotBeNullOrBlank(String name) {
        DropIndexCommandBuilder builder = DropIndexCommand.builder();

        builder.indexName("TEST")
                .schemaName(name);

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Name of the schema can't be null or blank"
        );
    }

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void indexNameMustNotBeNullOrBlank(String name) {
        DropIndexCommandBuilder builder = DropIndexCommand.builder();

        builder.schemaName("TEST")
                .indexName(name);

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Name of the index can't be null or blank"
        );
    }

    @Test
    void exceptionIsThrownIfSchemaNotExists() {
        Catalog catalog = catalogWithDefaultZone();

        CatalogCommand command = DropIndexCommand.builder()
                .schemaName(SCHEMA_NAME + "_UNK")
                .indexName("TEST")
                .build();

        assertThrowsWithCause(
                () -> command.get(new UpdateContext(catalog)),
                CatalogValidationException.class,
                "Schema with name 'PUBLIC_UNK' not found"
        );

        CatalogCommand dropCommand = DropIndexCommand.builder()
                .schemaName(SCHEMA_NAME + "_UNK")
                .indexName("TEST")
                .ifExists(true)
                .build();

        dropCommand.get(new UpdateContext(catalog)); // No exception is thrown
    }

    @Test
    void exceptionIsThrownIfIndexWithGivenNameNotFound() {
        Catalog catalog = catalogWithDefaultZone();

        CatalogCommand command = DropIndexCommand.builder()
                .schemaName(SCHEMA_NAME)
                .indexName("TEST")
                .build();

        assertThrowsWithCause(
                () -> command.get(new UpdateContext(catalog)),
                IndexNotFoundValidationException.class,
                "Index with name 'PUBLIC.TEST' not found"
        );
    }

    @Test
    void exceptionIsThrownIfIndexIsPrimaryKey() {
        ColumnParams columnParams = ColumnParams.builder().name("C").type(ColumnType.INT32).build();
        Catalog catalog = catalogWithDefaultZone(
                CreateTableCommand.builder()
                        .schemaName(SCHEMA_NAME)
                        .tableName(TABLE_NAME)
                        .columns(List.of(columnParams))
                        .primaryKey(primaryKey(columnParams.name()))
                        .build()
        );

        CatalogCommand command = DropIndexCommand.builder()
                .schemaName(SCHEMA_NAME)
                .indexName(pkIndexName(TABLE_NAME))
                .build();

        assertThrowsWithCause(
                () -> command.get(new UpdateContext(catalog)),
                CatalogValidationException.class,
                "Dropping primary key index is not allowed"
        );
    }
}
