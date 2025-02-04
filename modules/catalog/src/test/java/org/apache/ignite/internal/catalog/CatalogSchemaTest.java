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

import static org.apache.ignite.internal.catalog.CatalogTestUtils.columnParams;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.ASC_NULLS_LAST;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import org.apache.ignite.internal.catalog.commands.CreateSchemaCommand;
import org.apache.ignite.internal.catalog.commands.DropSchemaCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.sql.SqlCommon;
import org.junit.jupiter.api.Test;

/** Tests for schema related commands. */
public class CatalogSchemaTest extends BaseCatalogManagerTest {
    private static final String TEST_SCHEMA = "S1";

    @Test
    public void testCreateSchema() {
        tryApplyAndExpectApplied(CreateSchemaCommand.builder().name(TEST_SCHEMA).build());

        Catalog latestCatalog = latestCatalog();

        assertNotNull(latestCatalog.schema(TEST_SCHEMA));
        assertNotNull(latestCatalog.schema(SqlCommon.DEFAULT_SCHEMA_NAME));

        assertThat(
                manager.execute(CreateSchemaCommand.builder().name(TEST_SCHEMA).build()),
                willThrowFast(SchemaExistsValidationException.class, "Schema with name 'S1' already exists.")
        );

        tryApplyAndExpectNotApplied(CreateSchemaCommand.builder().name(TEST_SCHEMA).ifNotExists(true).build());
    }

    @Test
    public void testCreateSchemaIfNotExists() {
        {
            tryApplyAndExpectApplied(CreateSchemaCommand.builder().name(TEST_SCHEMA).ifNotExists(false).build());

            Catalog latestCatalog = latestCatalog();

            assertNotNull(latestCatalog.schema(TEST_SCHEMA));
            assertNotNull(latestCatalog.schema(SqlCommon.DEFAULT_SCHEMA_NAME));

            assertThat(
                    manager.execute(CreateSchemaCommand.builder().name(TEST_SCHEMA).build()),
                    willThrowFast(SchemaExistsValidationException.class, "Schema with name 'S1' already exists.")
            );
        }

        {
            tryApplyAndExpectApplied(CreateSchemaCommand.builder().name(TEST_SCHEMA + "_1").ifNotExists(false).build());

            Catalog latestCatalog = latestCatalog();

            assertNotNull(latestCatalog.schema(TEST_SCHEMA + "_1"));
            assertNotNull(latestCatalog.schema(TEST_SCHEMA));

            tryApplyAndExpectNotApplied(CreateSchemaCommand.builder().name(TEST_SCHEMA + "_1").ifNotExists(true).build());
        }
    }

    @Test
    public void testDropEmpty() {
        int initialSchemasCount = latestCatalog().schemas().size();

        tryApplyAndExpectApplied(CreateSchemaCommand.builder().name(TEST_SCHEMA).build());

        assertThat(latestCatalog().schemas(), hasSize(initialSchemasCount + 1));

        CatalogCommand cmd = DropSchemaCommand.builder().name(TEST_SCHEMA).build();

        tryApplyAndExpectApplied(cmd);
        assertThat(latestCatalog().schema(TEST_SCHEMA), nullValue());
        assertThat(latestCatalog().schemas(), hasSize(initialSchemasCount));

        assertThat(
                manager.execute(DropSchemaCommand.builder().name(TEST_SCHEMA).build()),
                willThrowFast(SchemaNotFoundValidationException.class, "Schema with name 'S1' not found.")
        );
    }

    @Test
    public void testDropIfExists() {
        tryApplyAndExpectNotApplied(DropSchemaCommand.builder().name(TEST_SCHEMA).ifExists(true).build());

        assertThat(
                manager.execute(DropSchemaCommand.builder().name(TEST_SCHEMA).ifExists(false).build()),
                willThrowFast(SchemaNotFoundValidationException.class, "Schema with name 'S1' not found.")
        );

        tryApplyAndExpectApplied(CreateSchemaCommand.builder().name(TEST_SCHEMA).build());

        tryApplyAndExpectApplied(DropSchemaCommand.builder().name(TEST_SCHEMA).ifExists(true).build());
        assertThat(latestCatalog().schema(TEST_SCHEMA), nullValue());
    }

    @Test
    public void testDropDefaultSchemaIsAllowed() {
        CatalogCommand cmd = DropSchemaCommand.builder().name(SqlCommon.DEFAULT_SCHEMA_NAME).build();

        tryApplyAndExpectApplied(cmd);
        assertThat(latestCatalog().schema(SqlCommon.DEFAULT_SCHEMA_NAME), nullValue());

        assertThat(
                manager.execute(simpleTable("test")),
                willThrowFast(SchemaNotFoundValidationException.class, "Schema with name 'PUBLIC' not found.")
        );
    }

    @Test
    public void testDropNonEmpty() {
        CatalogCommand newSchemaCmd = CreateSchemaCommand.builder().name(TEST_SCHEMA).build();
        CatalogCommand newTableCmd = newTableCommand("T1");
        CatalogCommand idxCmd = newIndexCommand("T1", "I1");

        tryApplyAndCheckExpect(
                List.of(newSchemaCmd, newTableCmd, idxCmd),
                true, true, true);

        // RESTRICT
        {
            assertThat(
                    manager.execute(DropSchemaCommand.builder().name(TEST_SCHEMA).build()),
                    willThrowFast(CatalogValidationException.class, "Schema 'S1' is not empty. Use CASCADE to drop it anyway.")
            );

            CatalogSchemaDescriptor schemaDescriptor = latestCatalog().schema(TEST_SCHEMA);

            assertThat(schemaDescriptor, is(notNullValue()));
            assertThat(schemaDescriptor.tables().length, is(1));
        }

        // CASCADE
        {
            assertThat(latestCatalog().tables(), hasSize(1));
            assertThat(latestCatalog().indexes(), hasSize(2));

            CatalogCommand dropCmd = DropSchemaCommand.builder().name(TEST_SCHEMA).cascade(true).build();

            tryApplyAndExpectApplied(dropCmd);

            assertThat(latestCatalog().schema(TEST_SCHEMA), nullValue());
            assertThat(latestCatalog().tables(), hasSize(0));
            assertThat(latestCatalog().indexes(), hasSize(0));
        }
    }

    private Catalog latestCatalog() {
        Catalog latestCatalog = manager.catalog(manager.activeCatalogVersion(clock.nowLong()));

        assertThat(latestCatalog, is(notNullValue()));

        return latestCatalog;
    }

    @SuppressWarnings("SameParameterValue")
    private static CatalogCommand newTableCommand(String tableName) {
        return createTableCommand(
                TEST_SCHEMA,
                tableName,
                List.of(columnParams("key1", INT32), columnParams("key2", INT32), columnParams("val", INT32, true)),
                List.of("key1", "key2"),
                List.of("key2")
        );
    }

    @SuppressWarnings("SameParameterValue")
    private static CatalogCommand newIndexCommand(String tableName, String indexName) {
        return createSortedIndexCommand(
                TEST_SCHEMA,
                tableName,
                indexName,
                false,
                List.of("key1"),
                List.of(ASC_NULLS_LAST)
        );
    }
}
