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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateSchemaCommand;
import org.apache.ignite.internal.catalog.commands.CreateTableCommand;
import org.apache.ignite.internal.catalog.commands.DropSchemaCommand;
import org.apache.ignite.internal.catalog.commands.TableHashPrimaryKey;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.sql.SqlCommon;
import org.junit.jupiter.api.Test;

/** Tests for schema related commands. */
public class CatalogSchemaTest extends BaseCatalogManagerTest {
    private static final String TEST_SCHEMA = "S1";

    @Test
    public void testCreateSchema() {
        assertThat(manager.execute(CreateSchemaCommand.builder().name(TEST_SCHEMA).build()), willCompleteSuccessfully());

        Catalog latestCatalog = latestCatalog();

        assertNotNull(latestCatalog.schema(TEST_SCHEMA));
        assertNotNull(latestCatalog.schema(SqlCommon.DEFAULT_SCHEMA_NAME));

        assertThat(
                manager.execute(CreateSchemaCommand.builder().name(TEST_SCHEMA).build()),
                willThrowFast(CatalogValidationException.class, "Schema with name 'S1' already exists")
        );
    }

    @Test
    public void testDropEmpty() {
        assertThat(manager.execute(CreateSchemaCommand.builder().name(TEST_SCHEMA).build()), willCompleteSuccessfully());

        CatalogCommand cmd = DropSchemaCommand.builder().name(TEST_SCHEMA).build();

        assertThat(manager.execute(cmd), willCompleteSuccessfully());
        assertThat(latestCatalog().schema(TEST_SCHEMA), nullValue());

        assertThat(
                manager.execute(DropSchemaCommand.builder().name(TEST_SCHEMA).build()),
                willThrowFast(CatalogValidationException.class, "Schema with name 'S1' not found")
        );
    }

    @Test
    public void testDropNonEmpty() {
        CatalogCommand newSchemaCmd = CreateSchemaCommand.builder().name(TEST_SCHEMA).build();
        CatalogCommand newTableCmd = newTableCmd();

        assertThat(manager.execute(List.of(newSchemaCmd, newTableCmd)), willCompleteSuccessfully());

        // RESTRICT
        {
            assertThat(
                    manager.execute(DropSchemaCommand.builder().name(TEST_SCHEMA).build()),
                    willThrowFast(CatalogValidationException.class, "Schema 'S1' is not empty. Use CASCADE to drop it anyway.")
            );

            Catalog latestCatalog = latestCatalog();
            CatalogSchemaDescriptor schemaDescriptor = latestCatalog.schema(TEST_SCHEMA);

            assertThat(schemaDescriptor, is(notNullValue()));
            assertThat(schemaDescriptor.tables().length, is(1));
        }

        // CASCADE
        {
            CatalogCommand dropCmd = DropSchemaCommand.builder().name(TEST_SCHEMA).cascade(true).build();

            assertThat(manager.execute(dropCmd), willCompleteSuccessfully());

            assertThat(latestCatalog().schema(TEST_SCHEMA), nullValue());
        }
    }

    private Catalog latestCatalog() {
        Catalog latestCatalog = manager.catalog(manager.activeCatalogVersion(clock.nowLong()));

        assertThat(latestCatalog, is(notNullValue()));

        return latestCatalog;
    }

    private static CatalogCommand newTableCmd() {
        return CreateTableCommand.builder()
                .schemaName(TEST_SCHEMA)
                .tableName("T1")
                .columns(List.of(ColumnParams.builder().name("ID").type(INT32).build()))
                .primaryKey(TableHashPrimaryKey.builder()
                        .columns(List.of("ID"))
                        .build())
                .build();
    }
}
