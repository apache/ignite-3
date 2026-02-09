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
import org.jetbrains.annotations.Nullable;

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
                .addColumn("SCHEMA_NAME", STRING, entry -> entry.schemaName)
                .addColumn("TABLE_NAME", STRING, entry -> entry.table.name())
                .addColumn("TABLE_ID", INT32, entry -> entry.table.id())
                .addColumn("TABLE_PK_INDEX_ID", INT32, entry -> entry.table.primaryKeyIndexId())
                .addColumn("ZONE_NAME", STRING, entry -> entry.zoneName)
                .addColumn("STORAGE_PROFILE", STRING, entry -> entry.table.storageProfile())
                .addColumn("TABLE_COLOCATION_COLUMNS", STRING, entry -> concatColumns(entry.table.colocationColumnNames()))
                .addColumn("SCHEMA_ID", INT32, entry -> entry.table.schemaId())
                .addColumn("ZONE_ID", INT32, entry -> entry.table.zoneId())
                // TODO https://issues.apache.org/jira/browse/IGNITE-24589: Next columns are deprecated and should be removed.
                //  They are kept for compatibility with 3.0 version, to allow columns being found by their old names.
                .addColumn("SCHEMA", STRING, entry -> entry.schemaName)
                .addColumn("NAME", STRING, entry -> entry.table.name())
                .addColumn("ID", INT32, entry -> entry.table.id())
                .addColumn("PK_INDEX_ID", INT32, entry -> entry.table.primaryKeyIndexId())
                .addColumn("COLOCATION_KEY_INDEX", STRING, entry -> concatColumns(entry.table.colocationColumnNames()))
                .addColumn("ZONE", STRING, entry -> entry.zoneName)
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
        Iterable<ColumnMetadata> viewData = () -> {
            Catalog catalog = catalogSupplier.get();

            return catalog.tables().stream()
                    .flatMap(table -> table.columns().stream()
                            .map(columnDescriptor -> new ColumnMetadata(
                                            catalog.schema(table.schemaId()).name(),
                                            table,
                                            columnDescriptor
                                    )
                            )
                    )
                    .iterator();
        };

        Publisher<ColumnMetadata> viewDataPublisher = SubscriptionUtils.fromIterable(viewData);

        return SystemViews.<ColumnMetadata>clusterViewBuilder()
                .name("TABLE_COLUMNS")
                .addColumn("SCHEMA_NAME", STRING, entry -> entry.schema)
                .addColumn("TABLE_NAME", STRING, entry -> entry.tableDescriptor.name())
                .addColumn("TABLE_ID", INT32, entry -> entry.tableDescriptor.id())
                .addColumn("COLUMN_NAME", STRING, entry -> entry.columnDescriptor.name())
                .addColumn("COLUMN_TYPE", STRING, entry -> entry.columnDescriptor.type().name())
                .addColumn("IS_NULLABLE_COLUMN", BOOLEAN, entry -> entry.columnDescriptor.nullable())
                .addColumn("COLUMN_PRECISION", INT32, entry -> entry.columnDescriptor.precision())
                .addColumn("COLUMN_SCALE", INT32, entry -> entry.columnDescriptor.scale())
                .addColumn("COLUMN_LENGTH", INT32, entry -> entry.columnDescriptor.length())
                .addColumn("COLUMN_ORDINAL", INT32, ColumnMetadata::columnOrdinal)
                .addColumn("SCHEMA_ID", INT32, entry -> entry.tableDescriptor.schemaId())
                .addColumn("PK_COLUMN_ORDINAL", INT32, ColumnMetadata::pkColumnOrdinal)
                .addColumn("COLOCATION_COLUMN_ORDINAL", INT32, ColumnMetadata::colocationColumnOrdinal)
                // TODO https://issues.apache.org/jira/browse/IGNITE-24589: Next columns are deprecated and should be removed.
                //  They are kept for compatibility with 3.0 version, to allow columns being found by their old names.
                .addColumn("SCHEMA", STRING, entry -> entry.schema)
                .addColumn("TYPE", STRING, entry -> entry.columnDescriptor.type().name())
                .addColumn("NULLABLE", BOOLEAN, entry -> entry.columnDescriptor.nullable())
                .addColumn("PREC", INT32, entry -> entry.columnDescriptor.precision())
                .addColumn("SCALE", INT32, entry -> entry.columnDescriptor.scale())
                .addColumn("LENGTH", INT32, entry -> entry.columnDescriptor.length())
                // End of legacy columns list. New columns must be added below this line.
                .dataProvider(viewDataPublisher)
                .build();
    }

    private static class ColumnMetadata {
        private final CatalogTableColumnDescriptor columnDescriptor;
        private final String schema;
        private final CatalogTableDescriptor tableDescriptor;

        private ColumnMetadata(
                String schema,
                CatalogTableDescriptor tableDescriptor,
                CatalogTableColumnDescriptor columnDescriptor
        ) {
            this.schema = schema;
            this.columnDescriptor = columnDescriptor;
            this.tableDescriptor = tableDescriptor;
        }

        int columnOrdinal() {
            return tableDescriptor.columnIndex(columnDescriptor.name());
        }

        @Nullable Integer pkColumnOrdinal() {
            int idx = tableDescriptor.primaryKeyColumnNames().indexOf(columnDescriptor.name());
            return idx >= 0 ? idx : null;
        }

        @Nullable Integer colocationColumnOrdinal() {
            int idx = tableDescriptor.colocationColumnNames().indexOf(columnDescriptor.name());
            return idx >= 0 ? idx : null;
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
