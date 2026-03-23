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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexCommand;
import org.apache.ignite.internal.catalog.commands.CreateSchemaCommand;
import org.apache.ignite.internal.catalog.commands.DropSchemaCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
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
                willThrowFast(CatalogValidationException.class, "Schema with name 'S1' already exists.")
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
                    willThrowFast(CatalogValidationException.class, "Schema with name 'S1' already exists.")
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
                willThrowFast(CatalogValidationException.class, "Schema with name 'S1' not found.")
        );
    }

    @Test
    public void testDropIfExists() {
        tryApplyAndExpectNotApplied(DropSchemaCommand.builder().name(TEST_SCHEMA).ifExists(true).build());

        assertThat(
                manager.execute(DropSchemaCommand.builder().name(TEST_SCHEMA).ifExists(false).build()),
                willThrowFast(CatalogValidationException.class, "Schema with name 'S1' not found.")
        );

        tryApplyAndExpectApplied(CreateSchemaCommand.builder().name(TEST_SCHEMA).build());

        tryApplyAndExpectApplied(DropSchemaCommand.builder().name(TEST_SCHEMA).ifExists(true).build());
        assertThat(latestCatalog().schema(TEST_SCHEMA), nullValue());
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

    @Test
    public void testSameTableNameInDifferentSchemas() {
        // Create table1 in schema1
        {
            CatalogCommand newSchemaCmd = CreateSchemaCommand.builder().name("S1").build();
            CatalogCommand newTableCmd = newTableCommand("S1", "T1");

            tryApplyAndCheckExpect(List.of(newSchemaCmd, newTableCmd), true, true);
        }

        // Create table1 in schema2
        {
            CatalogCommand newSchemaCmd = CreateSchemaCommand.builder().name("S2").build();
            CatalogCommand newTableCmd = newTableCommand("S2", "T1");

            tryApplyAndCheckExpect(List.of(newSchemaCmd, newTableCmd), true, true);
        }

        {
            CatalogSchemaDescriptor schema1 = latestCatalog().schema("S1");
            assertNotNull(schema1);

            CatalogSchemaDescriptor schema2 = latestCatalog().schema("S2");
            assertNotNull(schema2);

            CatalogTableDescriptor schema1table1 = schema1.table("T1");
            assertNotNull(schema1table1);

            CatalogTableDescriptor schema2table1 = schema2.table("T1");
            assertNotNull(schema2table1);
            assertNotEquals(schema1table1.id(), schema2table1.id(), "Table ids should differ");
        }
    }

    @Test
    public void testSameIndexNameInDifferentSchemas() {
        // Create table1 in schema1
        {
            CatalogCommand newSchemaCmd = CreateSchemaCommand.builder().name("S1").build();
            CatalogCommand newTableCmd = newTableCommand("S1", "T1");
            CatalogCommand newIndexCmd = CreateHashIndexCommand.builder()
                    .schemaName("S1")
                    .tableName("T1")
                    .indexName("MY_INDEX")
                    .columns(List.of("key2"))
                    .build();

            tryApplyAndCheckExpect(List.of(newSchemaCmd, newTableCmd, newIndexCmd), true, true, true);
        }

        // Create table1 in schema2
        {
            CatalogCommand newSchemaCmd = CreateSchemaCommand.builder().name("S2").build();
            CatalogCommand newTableCmd = newTableCommand("S2", "T2");
            CatalogCommand newIndexCmd = CreateHashIndexCommand.builder()
                    .schemaName("S2")
                    .tableName("T2")
                    .indexName("MY_INDEX")
                    .columns(List.of("key2"))
                    .build();

            tryApplyAndCheckExpect(List.of(newSchemaCmd, newTableCmd, newIndexCmd), true, true, true);
        }

        {
            CatalogSchemaDescriptor schema1 = latestCatalog().schema("S1");
            assertNotNull(schema1);

            CatalogSchemaDescriptor schema2 = latestCatalog().schema("S2");
            assertNotNull(schema2);

            CatalogIndexDescriptor schema1index = Arrays.stream(schema1.indexes())
                    .filter(i -> "MY_INDEX".equals(i.name()))
                    .findAny()
                    .orElseThrow();

            CatalogIndexDescriptor schema2index = Arrays.stream(schema2.indexes())
                    .filter(i -> "MY_INDEX".equals(i.name()))
                    .findAny()
                    .orElseThrow();

            assertNotEquals(schema1index.id(), schema2index.id(), "Index ids should differ");
        }
    }

    @Test
    public void testDropCreateEmptyDefaultSchema() {
        CatalogSchemaDescriptor publicSchema1 = latestCatalog().schema(SqlCommon.DEFAULT_SCHEMA_NAME);
        assertNotNull(publicSchema1);

        // Drop public schema
        CatalogCommand dropSchemaCmd = DropSchemaCommand.builder().name(SqlCommon.DEFAULT_SCHEMA_NAME).build();
        tryApplyAndExpectApplied(dropSchemaCmd);
        assertNull(latestCatalog().schema(SqlCommon.DEFAULT_SCHEMA_NAME));

        // Create public schema
        CatalogCommand newSchemaCmd = CreateSchemaCommand.builder().name(SqlCommon.DEFAULT_SCHEMA_NAME).build();
        tryApplyAndExpectApplied(newSchemaCmd);

        CatalogSchemaDescriptor publicSchema2 = latestCatalog().schema(SqlCommon.DEFAULT_SCHEMA_NAME);
        assertNotNull(publicSchema2);
        assertNotEquals(publicSchema1.id(), publicSchema2.id());
    }

    private Catalog latestCatalog() {
        Catalog latestCatalog = manager.catalog(manager.activeCatalogVersion(clock.nowLong()));

        assertThat(latestCatalog, is(notNullValue()));

        return latestCatalog;
    }

    private static CatalogCommand newTableCommand(String tableName) {
        return newTableCommand(TEST_SCHEMA, tableName);
    }

    private static CatalogCommand newTableCommand(String schemaName, String tableName) {
        return createTableCommand(
                schemaName,
                tableName,
                List.of(columnParams("key1", INT32), columnParams("key2", INT32), columnParams("val", INT32, true)),
                List.of("key1", "key2"),
                List.of("key2")
        );
    }

    private static CatalogCommand newIndexCommand(String tableName, String indexName) {
        return newIndexCommand(TEST_SCHEMA, tableName, indexName);
    }

    private static CatalogCommand newIndexCommand(String schemaName, String tableName, String indexName) {
        return createSortedIndexCommand(
                schemaName,
                tableName,
                indexName,
                false,
                List.of("key1"),
                List.of(ASC_NULLS_LAST)
        );
    }
}
