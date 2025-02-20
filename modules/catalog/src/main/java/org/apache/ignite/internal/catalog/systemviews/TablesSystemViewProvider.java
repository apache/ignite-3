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

package org.apache.ignite.internal.catalog.systemviews;

import static org.apache.ignite.internal.type.NativeTypes.BOOLEAN;
import static org.apache.ignite.internal.type.NativeTypes.INT32;
import static org.apache.ignite.internal.type.NativeTypes.STRING;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogSystemViewProvider;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViews;
import org.apache.ignite.internal.util.SubscriptionUtils;

/**
 * Exposes information on tables.
 *
 * <ul>
 *     <li>TABLES - available tables</li>
 *     <li>TABLE_COLUMNS - columns of available tables.</li>
 * </ul>
 */
public class TablesSystemViewProvider implements CatalogSystemViewProvider {
    @Override
    public List<SystemView<?>> getView(Supplier<Catalog> catalogSupplier) {
        return List.of(
                getTablesSystemView(catalogSupplier),
                getTableColumnsSystemView(catalogSupplier)
        );
    }

    private static SystemView<?> getTablesSystemView(Supplier<Catalog> catalogSupplier) {
        Iterable<TableWithSchemaAndZoneName> tablesData = () -> {
            Catalog catalog = catalogSupplier.get();

            return catalog.tables().stream().map(table -> {
                String schemaName = Objects.requireNonNull(catalog.schema(table.schemaId()), "Schema must be not null.").name();
                String zoneName = Objects.requireNonNull(catalog.zone(table.zoneId()), "Zone must be not null.").name();

                return new TableWithSchemaAndZoneName(table, schemaName, zoneName);
            }).iterator();
        };

        Publisher<TableWithSchemaAndZoneName> viewDataPublisher = SubscriptionUtils.fromIterable(tablesData);

        return SystemViews.<TableWithSchemaAndZoneName>clusterViewBuilder()
                .name("TABLES")
                .addColumn("SCHEMA", STRING, entry -> entry.schemaName)
                .addColumn("NAME", STRING, entry -> entry.table.name())
                .addColumn("ID", INT32, entry -> entry.table.id())
                .addColumn("PK_INDEX_ID", INT32, entry -> entry.table.primaryKeyIndexId())
                .addColumn("ZONE", STRING, entry -> entry.zoneName)
                .addColumn("STORAGE_PROFILE", STRING, entry -> entry.table.storageProfile())
                .addColumn("COLOCATION_KEY_INDEX", STRING, entry -> concatColumns(entry.table.colocationColumns()))
                .dataProvider(viewDataPublisher)
                .build();
    }

    private static String concatColumns(List<String> columns) {
        if (columns == null || columns.isEmpty()) {
            return "NULL";
        }
        return String.join(", ", columns);
    }

    private static SystemView<?> getTableColumnsSystemView(Supplier<Catalog> catalogSupplier) {
        Iterable<ColumnWithTableId> viewData = () -> {
            Catalog catalog = catalogSupplier.get();

            return catalog.tables().stream()
                    .flatMap(table -> table.columns().stream()
                            .map(columnDescriptor -> new ColumnWithTableId(
                                    catalog.schema(table.schemaId()).name(),
                                    table.name(),
                                    table.id(),
                                    columnDescriptor,
                                    table.columnIndex(columnDescriptor.name())
                                    )
                            )
                    )
                    .iterator();
        };

        Publisher<ColumnWithTableId> viewDataPublisher = SubscriptionUtils.fromIterable(viewData);

        return SystemViews.<ColumnWithTableId>clusterViewBuilder()
                .name("TABLE_COLUMNS")
                .addColumn("SCHEMA", STRING, entry -> entry.schema)
                .addColumn("TABLE_NAME", STRING, entry -> entry.tableName)
                .addColumn("TABLE_ID", INT32, entry -> entry.tableId)
                .addColumn("COLUMN_NAME", STRING, entry -> entry.descriptor.name())
                .addColumn("TYPE", STRING, entry -> entry.descriptor.type().name())
                .addColumn("NULLABLE", BOOLEAN, entry -> entry.descriptor.nullable())
                .addColumn("PREC", INT32, entry -> entry.descriptor.precision())
                .addColumn("SCALE", INT32, entry -> entry.descriptor.scale())
                .addColumn("LENGTH", INT32, entry -> entry.descriptor.length())
                .addColumn("COLUMN_ORDINAL", INT32, entry -> entry.columnOrdinal)
                .dataProvider(viewDataPublisher)
                .build();
    }

    private static class ColumnWithTableId {
        private final CatalogTableColumnDescriptor descriptor;
        private final String tableName;
        private final String schema;
        private final int tableId;
        private final int columnOrdinal;

        private ColumnWithTableId(
                String schema,
                String tableName,
                int tableId,
                CatalogTableColumnDescriptor descriptor,
                int columnOrdinal
        ) {
            this.schema = schema;
            this.tableName = tableName;
            this.descriptor = descriptor;
            this.tableId = tableId;
            this.columnOrdinal = columnOrdinal;
        }
    }

    private static class TableWithSchemaAndZoneName {
        private final CatalogTableDescriptor table;
        private final String schemaName;
        private final String zoneName;

        private TableWithSchemaAndZoneName(CatalogTableDescriptor table, String schemaName, String zoneName) {
            this.table = table;
            this.schemaName = schemaName;
            this.zoneName = zoneName;
        }
    }
}
