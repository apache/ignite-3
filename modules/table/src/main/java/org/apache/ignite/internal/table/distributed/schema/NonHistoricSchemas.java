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

package org.apache.ignite.internal.table.distributed.schema;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.descriptors.TableColumnDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaRegistry;

/**
 * A dummy implementation over {@link SchemaManager}. It is dummy because:
 *
 * <ul>
 *     <li>It imitates historicity, but always takes the latest known schema</li>
 *     <li>{@link #tableSchemaVersionsBetween(UUID, HybridTimestamp, HybridTimestamp)} always returns a single schema to avoid
 *     validation failures</li>
 * </ul>
 *
 * <p>The point of this implementation is to allow the system work in the pre-SchemaSync fashion before the switch to CatalogService
 * is possible.
 */
// TODO: IGNITE-19447 - remove when switched to the CatalogService
public class NonHistoricSchemas implements Schemas {
    private final SchemaManager schemaManager;

    public NonHistoricSchemas(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }

    @Override
    public CompletableFuture<?> waitForSchemasAvailability(HybridTimestamp ts) {
        return completedFuture(null);
    }

    @Override
    public List<FullTableSchema> tableSchemaVersionsBetween(UUID tableId, HybridTimestamp fromIncluding, HybridTimestamp toIncluding) {
        SchemaRegistry schemaRegistry = schemaManager.schemaRegistry(tableId);
        SchemaDescriptor schemaDescriptor = schemaRegistry.schema();

        List<TableColumnDescriptor> columns = schemaDescriptor.columnNames().stream()
                .map(colName -> {
                    Column column = schemaDescriptor.column(colName);

                    assert column != null;

                    return columnDescriptor(column);
                })
                .collect(toList());

        var fullSchema = new FullTableSchema(
                1,
                1,
                columns,
                List.of()
        );

        return List.of(fullSchema);
    }

    /**
     * Converts a {@link Column} to a {@link TableColumnDescriptor}.
     *
     * @param column Column to convert.
     * @return Conversion result.
     */
    public static TableColumnDescriptor columnDescriptor(Column column) {
        return new TableColumnDescriptor(
                column.name(),
                column.type().spec().asColumnType(),
                column.nullable(),
                DefaultValue.constant(column.defaultValue())
        );
    }
}
