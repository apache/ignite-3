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

package org.apache.ignite.internal.table.impl;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.table.distributed.schema.FullTableSchema;
import org.apache.ignite.internal.table.distributed.schema.NonHistoricSchemas;
import org.apache.ignite.internal.table.distributed.schema.Schemas;

/**
 * Dummy {@link Schemas} implementation that is not historic and always uses same {@link SchemaRegistry}.
 */
public class DummySchemas implements Schemas {
    private final SchemaRegistry schemaRegistry;

    public DummySchemas(SchemaRegistry schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    @Override
    public CompletableFuture<?> waitForSchemasAvailability(HybridTimestamp ts) {
        return completedFuture(null);
    }

    @Override
    public CompletableFuture<?> waitForSchemaAvailability(int tableId, int schemaVersion) {
        return completedFuture(null);
    }

    @Override
    public List<FullTableSchema> tableSchemaVersionsBetween(int tableId, HybridTimestamp fromIncluding, HybridTimestamp toIncluding) {
        SchemaDescriptor schemaDescriptor = schemaRegistry.lastKnownSchema();

        List<CatalogTableColumnDescriptor> columns = schemaDescriptor.columnNames().stream()
                .map(colName -> {
                    Column column = schemaDescriptor.column(colName);

                    assert column != null;

                    return NonHistoricSchemas.columnDescriptor(column);
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

    @Override
    public List<FullTableSchema> tableSchemaVersionsBetween(int tableId, HybridTimestamp fromIncluding, int toIncluding) {
        // Returning an empty list makes sure that backward validation never fails, which is what we want before
        // we switch to CatalogService completely.
        return List.of();
    }
}
