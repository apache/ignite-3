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

package org.apache.ignite.internal.catalog;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.ignite.internal.catalog.commands.CreateSchemaCommand;
import org.apache.ignite.internal.catalog.commands.DropSchemaCommand;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Validation tests for schema related commands. */
public class CatalogSchemaValidationTest extends BaseCatalogManagerTest {
    @Test
    public void testCreateSchemaWithExistingName() {
        assertThat(
                manager.execute(CreateSchemaCommand.builder().name(SqlCommon.DEFAULT_SCHEMA_NAME).build()),
                willThrowFast(CatalogValidationException.class, "Schema with name 'PUBLIC' already exists")
        );

        assertThat(
                manager.execute(CreateSchemaCommand.builder().name(SqlCommon.DEFAULT_SCHEMA_NAME).ifNotExists(false).build()),
                willThrowFast(CatalogValidationException.class, "Schema with name 'PUBLIC' already exists")
        );

        tryApplyAndExpectNotApplied(CreateSchemaCommand.builder().name(SqlCommon.DEFAULT_SCHEMA_NAME).ifNotExists(true).build());
    }

    @Test
    public void testDropNonExistingSchema() {
        assertThat(
                manager.execute(DropSchemaCommand.builder().name("NON_EXISTING").build()),
                willThrowFast(CatalogValidationException.class, "Schema with name 'NON_EXISTING' not found")
        );

        assertThat(
                manager.execute(DropSchemaCommand.builder().name("NON_EXISTING").ifExists(false).build()),
                willThrowFast(CatalogValidationException.class, "Schema with name 'NON_EXISTING' not found")
        );

        tryApplyAndExpectNotApplied(DropSchemaCommand.builder().name("NON_EXISTING").ifExists(true).build());
    }

    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testCreateSchemaWithNullOrEmptyNameIsRejected() {
        //noinspection DataFlowIssue
        IgniteTestUtils.assertThrows(
                CatalogValidationException.class,
                () -> manager.execute(CreateSchemaCommand.builder().name(null).build()),
                "Name of the schema can't be null or blank"
        );

        IgniteTestUtils.assertThrows(
                CatalogValidationException.class,
                () -> manager.execute(CreateSchemaCommand.builder().name("").build()),
                "Name of the schema can't be null or blank"
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {
            CatalogService.SYSTEM_SCHEMA_NAME,
            CatalogService.INFORMATION_SCHEMA,
            CatalogService.DEFINITION_SCHEMA
    })
    public void testDropSystemSchemaIsForbidden(String schemaName) {
        CatalogCommand dropCmd = DropSchemaCommand.builder().name(schemaName).build();

        assertThat(
                manager.execute(dropCmd),
                willThrowFast(CatalogValidationException.class, format("System schema can't be dropped [name={}]", schemaName))
        );
    }

    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testDropSchemaWithNullOrEmptyNameIsRejected() {
        //noinspection DataFlowIssue
        IgniteTestUtils.assertThrows(
                CatalogValidationException.class,
                () -> manager.execute(DropSchemaCommand.builder().name(null).build()),
                "Name of the schema can't be null or blank"
        );

        IgniteTestUtils.assertThrows(
                CatalogValidationException.class,
                () -> manager.execute(DropSchemaCommand.builder().name("").build()),
                "Name of the schema can't be null or blank"
        );
    }
}
