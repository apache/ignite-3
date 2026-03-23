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

import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.UpdateContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests to verify validation of {@link AlterTableSetPropertyCommand}.
 */
@SuppressWarnings("ThrowableNotThrown")
public class AlterTableSetPropertyCommandValidationTest extends AbstractCommandValidationTest {
    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void schemaNameMustNotBeNullOrBlank(String name) {
        AlterTableSetPropertyCommandBuilder builder = AlterTableSetPropertyCommand.builder();

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
        AlterTableSetPropertyCommandBuilder builder = AlterTableSetPropertyCommand.builder();

        builder = fillProperties(builder);

        builder.tableName(name);

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Name of the table can't be null or blank"
        );
    }

    @ParameterizedTest
    @ValueSource(doubles = {
            Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, -1, 1.1
    })
    void staleRowsFractionShouldBeInValidRange(double staleRowsFraction) {
        AlterTableSetPropertyCommandBuilder builder = AlterTableSetPropertyCommand.builder();

        builder = fillProperties(builder)
                .staleRowsFraction(staleRowsFraction);

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Stale rows fraction should be in range [0, 1]."
        );
    }

    @ParameterizedTest
    @ValueSource(longs = {
            -100, -10, -1
    })
    void minStaleRowsCountShouldBeNonNegative(long minStaleRowsCount) {
        AlterTableSetPropertyCommandBuilder builder = AlterTableSetPropertyCommand.builder();

        builder = fillProperties(builder)
                .minStaleRowsCount(minStaleRowsCount);

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Minimal stale rows count should be non-negative."
        );
    }

    private static AlterTableSetPropertyCommandBuilder fillProperties(AlterTableSetPropertyCommandBuilder builder) {
        return builder
                .schemaName(SCHEMA_NAME)
                .tableName("TEST");
    }

    @Test
    void exceptionIsThrownIfSchemaNotExists() {
        AlterTableSetPropertyCommandBuilder builder = AlterTableSetPropertyCommand.builder();

        Catalog catalog = catalogWithDefaultZone();

        CatalogCommand command = fillProperties(builder).schemaName(SCHEMA_NAME + "_UNK").build();

        assertThrowsWithCause(
                () -> command.get(new UpdateContext(catalog)),
                CatalogValidationException.class,
                "Schema with name 'PUBLIC_UNK' not found"
        );

        CatalogCommand alterCommand = builder.ifTableExists(true).build();

        alterCommand.get(new UpdateContext(catalog)); // No exception
    }

    @Test
    void exceptionIsThrownIfTableNotExists() {
        AlterTableSetPropertyCommandBuilder builder = AlterTableSetPropertyCommand.builder();

        Catalog catalog = catalogWithDefaultZone();

        CatalogCommand command = fillProperties(builder).tableName("TEST").build();

        assertThrowsWithCause(
                () -> command.get(new UpdateContext(catalog)),
                CatalogValidationException.class,
                "Table with name 'PUBLIC.TEST' not found"
        );
    }

    @ParameterizedTest
    @MethodSource("reservedSchemaNames")
    void exceptionIsThrownIfSchemaIsReserved(String schema) {
        AlterTableSetPropertyCommandBuilder builder = AlterTableSetPropertyCommand.builder();

        builder.schemaName(schema)
                .tableName("t");

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Operations with system schemas are not allowed"
        );
    }
}
