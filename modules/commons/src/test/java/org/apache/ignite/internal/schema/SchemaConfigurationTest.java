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

import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.SchemaTable;
import org.apache.ignite.schema.builder.SchemaTableBuilder;
import org.junit.jupiter.api.Test;

public class SchemaConfigurationTest {
    @Test
    public void testInitialSchema() {
        //TODO: Do we need separate 'Schema builder' or left 'schema' name as kind of 'namespace'.
        final SchemaTableBuilder builder = SchemaBuilders.tableBuilder("PUBLIC", "table1");

        builder
            .columns(
                // Declaring columns in user order.
                SchemaBuilders.column("id", ColumnType.INT64).build(),
                SchemaBuilders.column("label", ColumnType.stringOf(2)).withDefaultValue("AI").build(),
                SchemaBuilders.column("name", ColumnType.string()).asNonNull().build(),
                SchemaBuilders.column("data", ColumnType.blobOf(255)).asNullable().build(),
                SchemaBuilders.column("affId", ColumnType.INT32).build()
            )

            // PK index type can't be changed as highly coupled core implementation.
            .pkColumns("id", "affId", "label") // Declare index column in order.
            .affinityColumns("affId") // Optional affinity declaration. If not set, all columns will be affinity cols.

            // 'withIndex' single entry point allows extended index support.
            // E.g. we may want to support GEO indices later with some plugin.
            .withindex(
                SchemaBuilders.sortedIndex("idx_1_sorted")
                    .addIndexColumn("id").desc().done()
                    .addIndexColumn("name").asc().done()
                    .withInlineSize(42)
                    .build()
            )

            .withindex(
                SchemaBuilders.partialIndex("idx_2_partial")
                    .addIndexColumn("id").desc().done()
                    .addIndexColumn("name").asc().done()
                    .withExpression("id > 0")
                    .withInlineSize(42)
                    .build()
            )

            .withindex(
                SchemaBuilders.hashIndex("idx_3_hash")
                    .withColumns("id", "affId")
                    .build()
            )

            .build();
    }

    @Test
    public void testSchemaModification() {
        final SchemaTable table = SchemaBuilders.tableBuilder("PUBLIC", "table1")
            .columns(
                // Declaring columns in user order.
                SchemaBuilders.column("id", ColumnType.INT64).build(),
                SchemaBuilders.column("name", ColumnType.string()).build()
            )
            .pkColumns("id")
            .build();

        table.toBuilder()
            .addColumn(
                SchemaBuilders.column("firstName", ColumnType.string())
                    .asNonNull()
                    .build()
            )
            .addKeyColumn( // It looks safe to add non-affinity column to key.
                SchemaBuilders.column("subId", ColumnType.string())
                    .asNonNull()
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
                    .build()
            )

            .dropIndex("hash_idx")
            .apply();
    }
}
