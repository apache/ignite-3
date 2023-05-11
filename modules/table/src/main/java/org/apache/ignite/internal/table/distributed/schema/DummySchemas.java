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
import static org.apache.ignite.lang.ErrorGroups.Client.PROTOCOL_ERR;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.descriptors.TableColumnDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.ColumnType;

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
public class DummySchemas implements Schemas {
    private final SchemaManager schemaManager;

    public DummySchemas(SchemaManager schemaManager) {
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
        return getColumnType(type.spec());
    }

    private static ColumnType getColumnType(NativeTypeSpec spec) {
        switch (spec) {
            case INT8:
                return ColumnType.INT8;

            case INT16:
                return ColumnType.INT16;

            case INT32:
                return ColumnType.INT32;

            case INT64:
                return ColumnType.INT64;

            case FLOAT:
                return ColumnType.FLOAT;

            case DOUBLE:
                return ColumnType.DOUBLE;

            case DECIMAL:
                return ColumnType.DECIMAL;

            case NUMBER:
                return ColumnType.NUMBER;

            case UUID:
                return ColumnType.UUID;

            case STRING:
                return ColumnType.STRING;

            case BYTES:
                return ColumnType.BYTE_ARRAY;

            case BITMASK:
                return ColumnType.BITMASK;

            case DATE:
                return ColumnType.DATE;

            case TIME:
                return ColumnType.TIME;

            case DATETIME:
                return ColumnType.DATETIME;

            case TIMESTAMP:
                return ColumnType.TIMESTAMP;

            default:
                throw new IgniteException(PROTOCOL_ERR, "Unsupported native type: " + spec);
        }
    }
}
