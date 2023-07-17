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
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BitmaskNativeType;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.DecimalNativeType;
import org.apache.ignite.internal.schema.DefaultValueProvider;
import org.apache.ignite.internal.schema.DefaultValueProvider.FunctionalValueProvider;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.TemporalNativeType;
import org.apache.ignite.internal.schema.VarlenNativeType;

/**
 * A dummy implementation over {@link SchemaManager}. It is dummy because:
 *
 * <ul>
 *     <li>It imitates historicity, but always takes the latest known schema</li>
 *     <li>{@link #tableSchemaVersionsBetween(int, HybridTimestamp, HybridTimestamp)} always returns a single schema to avoid
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
    public CompletableFuture<?> waitForSchemaAvailability(int tableId, int schemaVersion) {
        return completedFuture(null);
    }

    @Override
    public List<FullTableSchema> tableSchemaVersionsBetween(int tableId, HybridTimestamp fromIncluding, HybridTimestamp toIncluding) {
        SchemaRegistry schemaRegistry = schemaManager.schemaRegistry(tableId);
        SchemaDescriptor schemaDescriptor = schemaRegistry.schema();

        List<CatalogTableColumnDescriptor> columns = schemaDescriptor.columnNames().stream()
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

    @Override
    public List<FullTableSchema> tableSchemaVersionsBetween(int tableId, HybridTimestamp fromIncluding, int toIncluding) {
        // Returning an empty list makes sure that backward validation never fails, which is what we want before
        // we switch to CatalogService completely.
        return List.of();
    }

    /**
     * Converts a {@link Column} to a {@link CatalogTableColumnDescriptor}. Please note that the conversion is not full; it's
     * used in the code that actually doesn't care about columns.
     *
     * @param column Column to convert.
     * @return Conversion result.
     */
    public static CatalogTableColumnDescriptor columnDescriptor(Column column) {
        NativeType nativeType = column.type();
        int precision;
        int scale;
        int length;

        switch (nativeType.spec()) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
            case NUMBER:
            case DATE:
            case UUID:
            case BOOLEAN:
                precision = 0;
                scale = 0;
                length = 0;
                break;
            case DECIMAL:
                DecimalNativeType decimalNativeType = (DecimalNativeType) nativeType;
                precision = decimalNativeType.precision();
                scale = decimalNativeType.scale();
                length = 0;
                break;
            case STRING:
            case BYTES:
                VarlenNativeType varlenNativeType = (VarlenNativeType) nativeType;
                precision = 0;
                scale = 0;
                length = varlenNativeType.length();
                break;
            case BITMASK:
                BitmaskNativeType bitmaskNativeType = (BitmaskNativeType) nativeType;
                precision = 0;
                scale = 0;
                length = bitmaskNativeType.bits();
                break;
            case TIME:
            case DATETIME:
            case TIMESTAMP:
                TemporalNativeType temporalNativeType = (TemporalNativeType) nativeType;
                precision = temporalNativeType.precision();
                scale = 0;
                length = 0;
                break;
            default:
                throw new IllegalArgumentException("Unexpected native type: " + nativeType);
        }

        return new CatalogTableColumnDescriptor(
                column.name(),
                nativeType.spec().asColumnType(),
                column.nullable(),
                precision,
                scale,
                length,
                defaultValue(column.defaultValueProvider())
        );
    }

    private static DefaultValue defaultValue(DefaultValueProvider defaultValueProvider) {
        if (defaultValueProvider instanceof FunctionalValueProvider) {
            FunctionalValueProvider functionalProvider = (FunctionalValueProvider) defaultValueProvider;
            return DefaultValue.functionCall(functionalProvider.name());
        } else {
            return DefaultValue.constant(defaultValueProvider.get());
        }
    }
}
