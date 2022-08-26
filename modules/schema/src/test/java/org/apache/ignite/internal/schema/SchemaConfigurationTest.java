/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.schema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.SchemaObject;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.schema.definition.builder.TableDefinitionBuilder;
import org.junit.jupiter.api.Test;

/**
 * SchemaConfigurationTest.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class SchemaConfigurationTest {
    /**
     * TestInitialSchema.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @Test
    public void testInitialSchema() {
        final TableDefinitionBuilder builder = SchemaBuilders.tableBuilder(SchemaObject.DEFAULT_DATABASE_SCHEMA_NAME, "table1");

        builder
                .columns(
                        // Declaring columns in user order.
                        SchemaBuilders.column("id", ColumnType.INT64).build(),
                        SchemaBuilders.column("label", ColumnType.stringOf(2)).withDefaultValue("AI").build(),
                        SchemaBuilders.column("name", ColumnType.string()).build(),
                        SchemaBuilders.column("data", ColumnType.blobOf(255)).asNullable(true).build(),
                        SchemaBuilders.column("affId", ColumnType.INT32).build()
                )

                .withPrimaryKey(
                        SchemaBuilders.primaryKey()  // Declare index column in order.
                                .withColumns("id", "affId", "name")
                                .withColocationColumns(
                                        "affId") // Optional colocation declaration. If not set, all PK columns will be colocation cols.
                                .build()
                )

                // 'withIndex' single entry point allows extended index support.
                // E.g. we may want to support Geo-index later with some plugin.
                .withIndex(
                        SchemaBuilders.sortedIndex("idx_1_sorted")
                                .addIndexColumn("id").desc().done()
                                .addIndexColumn("name").asc().done()
                                .withHints(Map.of("INLINE_SIZE", "42", "INLINE_STRATEGY", "INLINE_HASH")) // In-line key-hash as well.
                                .build()
                )

                .withIndex(
                        SchemaBuilders.hashIndex("idx_3_hash")
                                .withColumns("id", "affId")
                                .build()
                )

                .build();
    }

    /**
     * TestSchemaModification.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @Test
    public void testSchemaModification() {
        final TableDefinition table = SchemaBuilders.tableBuilder("PUBLIC", "table1")
                .columns(
                        // Declaring columns in user order.
                        SchemaBuilders.column("id", ColumnType.INT64).build(),
                        SchemaBuilders.column("name", ColumnType.string()).build()
                )
                .withPrimaryKey("id")
                .build();

        table.toBuilder()
                .addColumn(
                        SchemaBuilders.column("firstName", ColumnType.string())
                                .build()
                )
                .addKeyColumn(
                        // It looks safe to add non-affinity column to key.
                        SchemaBuilders.column("subId", ColumnType.string())
                                .build()
                )

                .alterColumn("firstName")
                .withNewName("lastName")
                .withNewDefault("ivanov")
                .asNullable()
                .convertTo(ColumnType.stringOf(100))
                .done()

                .dropColumn("name") // Key column can't be dropped.

                .addIndex(
                        SchemaBuilders.sortedIndex("sortedIdx")
                                .addIndexColumn("subId").done()
                                .withHints(Map.of("INLINE_SIZE", "73"))
                                .build()
                )

                .dropIndex("hash_idx")
                .apply();
    }

    /**
     * Check invalid colocation columns configuration:
     * - not PK columns;
     * - duplicates colocation columns.
     */
    @Test
    public void invalidColocationColumns() {
        assertThat(
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                SchemaBuilders.primaryKey()  // Declare index column in order.
                                        .withColumns("id0", "id1", "id2")
                                        .withColocationColumns(
                                                "val")
                                        .build(),
                        "Schema definition error: All colocation columns must be part of key."
                ).getMessage(),
                containsString("Schema definition error: All colocation columns must be part of primary key")
        );

        assertThat(
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                SchemaBuilders.primaryKey()  // Declare index column in order.
                                        .withColumns("id0", "id1", "id2")
                                        .withColocationColumns(
                                                "id0, id1, id0")
                                        .build(),
                        "Schema definition error: Colocation columns must not be duplicated."
                ).getMessage(),
                containsString("Schema definition error: All colocation columns must be part of primary key")
        );
    }
}
