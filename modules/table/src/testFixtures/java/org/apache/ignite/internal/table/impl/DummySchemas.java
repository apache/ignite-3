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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.descriptors.TableColumnDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.table.distributed.schema.FullTableSchema;
import org.apache.ignite.internal.table.distributed.schema.NonHistoricSchemas;
import org.apache.ignite.internal.table.distributed.schema.Schemas;
import org.apache.ignite.sql.ColumnType;

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
    public List<FullTableSchema> tableSchemaVersionsBetween(UUID tableId, HybridTimestamp fromIncluding, HybridTimestamp toIncluding) {
        SchemaDescriptor schemaDescriptor = schemaRegistry.schema();
        FullTableSchema fullSchema = new FullTableSchema(
                1,
                1,
                schemaDescriptor.columnNames().stream()
                        .map(colName -> {
                            Column column = schemaDescriptor.column(colName);

                            assert column != null;

                            return columnType(colName, column);
                        })
                        .collect(toList()),
                List.of()
        );

        return List.of(fullSchema);
    }

    private static TableColumnDescriptor columnType(String colName, Column column) {
        return new TableColumnDescriptor(
                colName,
                columnTypeFromNativeType(column.type()),
                column.nullable(),
                DefaultValue.constant(column.defaultValue())
        );
    }

    private static ColumnType columnTypeFromNativeType(NativeType type) {
        return NonHistoricSchemas.getColumnType(type.spec());
    }
}
