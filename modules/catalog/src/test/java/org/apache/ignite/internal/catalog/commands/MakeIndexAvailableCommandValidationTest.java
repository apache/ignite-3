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

import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.IndexAlreadyAvailableValidationException;
import org.apache.ignite.internal.catalog.IndexNotFoundValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests to verify validation of {@link MakeIndexAvailableCommand}. */
@SuppressWarnings("ThrowableNotThrown")
public class MakeIndexAvailableCommandValidationTest extends AbstractCommandValidationTest {
    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void schemaNameMustNotBeNullOrBlank(String name) {
        MakeIndexAvailableCommandBuilder builder = MakeIndexAvailableCommand.builder();

        builder.indexName("TEST").schemaName(name);

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Name of the schema can't be null or blank"
        );
    }

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void indexNameMustNotBeNullOrBlank(String name) {
        MakeIndexAvailableCommandBuilder builder = MakeIndexAvailableCommand.builder();

        builder.schemaName(SCHEMA_NAME).indexName(name);

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Name of the index can't be null or blank"
        );
    }

    @Test
    void exceptionIsThrownIfIndexWithGivenNameNotFound() {
        Catalog catalog = emptyCatalog();

        CatalogCommand command = MakeIndexAvailableCommand.builder()
                .schemaName(SCHEMA_NAME)
                .indexName("TEST")
                .build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                IndexNotFoundValidationException.class,
                "Index with name 'PUBLIC.TEST' not found"
        );
    }

    @Test
    void exceptionIsThrownIfIndexAlreadyAvailable() {
        String indexName = "TEST";

        Catalog catalog = catalog(
                new CatalogTableDescriptor[]{},
                new CatalogIndexDescriptor[]{
                        new CatalogHashIndexDescriptor(10, indexName, 1, false, List.of("c"), false)
                },
                new CatalogSystemViewDescriptor[]{}
        );

        CatalogCommand command = MakeIndexAvailableCommand.builder()
                .schemaName(SCHEMA_NAME)
                .indexName(indexName)
                .build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                IndexAlreadyAvailableValidationException.class,
                "Index already available 'PUBLIC.TEST'"
        );
    }
}
