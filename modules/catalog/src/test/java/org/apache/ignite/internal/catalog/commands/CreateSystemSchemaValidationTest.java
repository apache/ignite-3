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

import static org.apache.ignite.internal.catalog.CatalogService.INFORMATION_SCHEMA;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;

import java.util.Locale;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.UpdateContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify validation of {@link CreateSchemaCommand} for system schemas.
 */
@SuppressWarnings({"ThrowableNotThrown"})
public class CreateSystemSchemaValidationTest extends AbstractCommandValidationTest {

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void schemaNameMustNotBeNullOrBlank(String name) {
        CreateSystemSchemaCommandBuilder builder = CreateSchemaCommand.systemSchemaBuilder().name(name);

        assertThrows(
                CatalogValidationException.class,
                builder::build,
                "Name of the schema can't be null or blank"
        );
    }

    @Test
    void commandFailsWithNonSystemSchema() {
        String schemaName = INFORMATION_SCHEMA.toLowerCase(Locale.ROOT);

        Catalog catalog = catalogWithSchema(INFORMATION_SCHEMA);

        assertThrows(
                CatalogValidationException.class,
                () -> CreateSchemaCommand.systemSchemaBuilder().name(schemaName).build().get(new UpdateContext(catalog)),
                format("Not a system schema, schema: '{}'", schemaName)
        );
    }

    @Test
    void commandFailsWhenSchemaAlreadyExists() {
        CreateSystemSchemaCommandBuilder builder = CreateSchemaCommand.systemSchemaBuilder().name(INFORMATION_SCHEMA);

        Catalog catalog = catalogWithSchema(INFORMATION_SCHEMA);

        assertThrows(
                CatalogValidationException.class,
                () -> builder.build().get(new UpdateContext(catalog)),
                format("Schema with name '{}' already exists", INFORMATION_SCHEMA)
        );
    }

    private static Catalog catalogWithSchema(String schemaName) {
        return catalogWithDefaultZone(CreateSchemaCommand.systemSchemaBuilder().name(schemaName).build());
    }
}
