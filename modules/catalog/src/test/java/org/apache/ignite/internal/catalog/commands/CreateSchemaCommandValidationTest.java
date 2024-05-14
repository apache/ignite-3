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
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify validation of {@link CreateSchemaCommand}.
 */
public class CreateSchemaCommandValidationTest extends AbstractCommandValidationTest {

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void schemaNameMustNotBeNullOrBlank(String name) {
        CreateSchemaCommandBuilder builder = CreateSchemaCommand.builder().name(name);

        assertThrows(
                CatalogValidationException.class,
                builder::build,
                "Name of the schema can't be null or blank"
        );
    }

    @Test
    void commandFailsWhenSchemaAlreadyExists() {
        String schemaName = "TEST";

        CreateSchemaCommandBuilder builder = CreateSchemaCommand.builder().name(schemaName);

        Catalog catalog = catalogWithSchema(schemaName);

        assertThrows(
                CatalogValidationException.class,
                () -> builder.build().get(catalog),
                "Schema with name 'TEST' already exists"
        );
    }

    private static Catalog catalogWithSchema(String schemaName) {
        return catalog(CreateSchemaCommand.builder().name(schemaName).build());
    }
}
