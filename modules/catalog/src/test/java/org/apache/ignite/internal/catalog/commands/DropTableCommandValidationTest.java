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

import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.UpdateProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify validation of {@link DropTableCommand}.
 */
public class DropTableCommandValidationTest extends AbstractCommandValidationTest {
    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void schemaNameMustNotBeNullOrBlank(String name) {
        DropTableCommandBuilder builder = manager.dropTableCommandBuilder();

        builder.tableName("TEST")
                .schemaName(name);

        assertThrows(
                builder::build,
                CatalogValidationException.class,
                "Name of the schema can't be null or blank"
        );
    }

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void tableNameMustNotBeNullOrBlank(String name) {
        DropTableCommandBuilder builder = manager.dropTableCommandBuilder();

        builder.schemaName("TEST")
                .tableName(name);

        assertThrows(
                builder::build,
                CatalogValidationException.class,
                "Name of the table can't be null or blank"
        );
    }

    @Test
    void exceptionIsThrownIfSchemaNotExists() {
        DropTableCommandBuilder builder = manager.dropTableCommandBuilder();

        Catalog catalog = emptyCatalog();

        UpdateProducer updateProducer = (UpdateProducer) builder
                .schemaName(SCHEMA_NAME + "_UNK")
                .tableName("TEST")
                .build();

        assertThrows(
                () -> updateProducer.get(catalog),
                CatalogValidationException.class,
                "Schema with name 'PUBLIC_UNK' not found"
        );
    }

    @Test
    void exceptionIsThrownIfTableWithGivenNameNotFound() {
        DropTableCommandBuilder builder = manager.dropTableCommandBuilder();

        Catalog catalog = emptyCatalog();

        UpdateProducer updateProducer = (UpdateProducer) builder
                .schemaName(SCHEMA_NAME)
                .tableName("TEST")
                .build();

        assertThrows(
                () -> updateProducer.get(catalog),
                CatalogValidationException.class,
                "Table with name 'PUBLIC.TEST' not found"
        );
    }
}
