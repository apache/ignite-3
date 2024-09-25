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
import static org.apache.ignite.internal.type.NativeTypes.stringOf;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogSystemViewProvider;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViews;
import org.apache.ignite.internal.util.SubscriptionUtils;

/**
 * Exposes information on tables.
 *
 * <ul>
 *     <li>TABLES - available tables</li>
 *     <li>TABLES_COLUMNS - columns of available tables.</li>
 *     <li>TABLES_COLOCATION_COLUMNS - colocated columns of available tables.</li>
 * </ul>
 */
public class TablesSystemViewProvider implements CatalogSystemViewProvider {
    private static final int SYSTEM_VIEW_STRING_COLUMN_LENGTH = Short.MAX_VALUE;

    @Override
    public List<SystemView<?>> getView(Supplier<Catalog> catalogSupplier) {
        return List.of(
                getSystemViewView(catalogSupplier),
                getSystemViewColocationColumnsView(catalogSupplier),
                getSystemViewColumnsView(catalogSupplier)
        );
    }

    private static SystemView<?> getSystemViewView(Supplier<Catalog> catalogSupplier) {
        Iterable<Info> tablesData = () -> {
            Catalog catalog = catalogSupplier.get();

            return catalog.tables().stream().map(table -> {
                String tableName = table.name();
                String schemaName = Objects.requireNonNull(catalog.schema(table.schemaId()), "Schema must be not null.").name();
                String zoneName = Objects.requireNonNull(catalog.zone(table.zoneId()), "Zone must be not null.").name();

                int indexId = table.primaryKeyIndexId();

                return new Info(tableName, schemaName, indexId, zoneName);
            }).iterator();
        };

        Publisher<Info> viewDataPublisher = SubscriptionUtils.fromIterable(tablesData);

        return SystemViews.<Info>clusterViewBuilder()
                .name("TABLES")
                .addColumn("SCHEMA", STRING, entry -> entry.schema)
                .addColumn("NAME", STRING, entry -> entry.name)
                .addColumn("PK_INDEX_ID", INT32, entry -> entry.pkIndexId)
                .addColumn("ZONE", STRING, entry -> entry.zone)
                .dataProvider(viewDataPublisher)
                .build();
    }

    private static SystemView<?> getSystemViewColumnsView(Supplier<Catalog> catalogSupplier) {
        Iterable<ColumnWithTableId> viewData = () -> {
            Catalog catalog = catalogSupplier.get();

            return catalog.tables().stream()
                    .flatMap(table -> table.columns().stream()
                            .map(columnDescriptor -> new ColumnWithTableId(
                                    catalog.schema(table.schemaId()).name(),
                                    table.name(),
                                    columnDescriptor
                                    )
                            )
                    )
                    .iterator();
        };

        Publisher<ColumnWithTableId> viewDataPublisher = SubscriptionUtils.fromIterable(viewData);

        return SystemViews.<ColumnWithTableId>clusterViewBuilder()
                .name("TABLES_COLUMNS")
                .addColumn("SCHEMA", STRING, entry -> entry.schema)
                .addColumn("TABLE_NAME", STRING, entry -> entry.tableName)
                .addColumn("COLUMN_NAME", STRING, entry -> entry.descriptor.name())
                .addColumn("TYPE", STRING, entry -> entry.descriptor.type().name())
                .addColumn("NULLABLE", BOOLEAN, entry -> entry.descriptor.nullable())
                .addColumn("PRECISION", INT32, entry -> entry.descriptor.precision())
                .addColumn("SCALE", INT32, entry -> entry.descriptor.scale())
                .addColumn("LENGTH", INT32, entry -> entry.descriptor.length())
                .dataProvider(viewDataPublisher)
                .build();
    }

    private static SystemView<?> getSystemViewColocationColumnsView(Supplier<Catalog> catalogSupplier) {
        Iterable<ColocationColumnsWithTable> viewData = () -> {
            Catalog catalog = catalogSupplier.get();

            return catalog.tables().stream()
                    .flatMap(table -> table.colocationColumns().stream()
                            .map(colocationColumn -> new ColocationColumnsWithTable(table.name(), colocationColumn))
                    )
                    .iterator();
        };

        Publisher<ColocationColumnsWithTable> viewDataPublisher = SubscriptionUtils.fromIterable(viewData);

        return SystemViews.<ColocationColumnsWithTable>clusterViewBuilder()
                .name("TABLES_COLOCATION_COLUMNS")
                .addColumn("TABLE_NAME", STRING, entry -> entry.tableName)
                .addColumn("COLOCATION_COLUMN", stringOf(SYSTEM_VIEW_STRING_COLUMN_LENGTH), entry -> entry.colocationColumn)
                .dataProvider(viewDataPublisher)
                .build();
    }

    private static class ColumnWithTableId {
        private final CatalogTableColumnDescriptor descriptor;
        private final String tableName;
        private final String schema;

        private ColumnWithTableId(String schema, String tableName, CatalogTableColumnDescriptor descriptor) {
            this.schema = schema;
            this.tableName = tableName;
            this.descriptor = descriptor;
        }
    }

    private static class ColocationColumnsWithTable {
        private final String tableName;
        private final String colocationColumn;

        private ColocationColumnsWithTable(String tableName, String colocationColumn) {
            this.tableName = tableName;
            this.colocationColumn = colocationColumn;
        }
    }

    private static class Info {
        private final String name;
        private final String schema;
        private final int pkIndexId;
        private final String zone;

        private Info(String name, String schema, int pkIndexId, String zone) {
            this.name = name;
            this.schema = schema;
            this.pkIndexId = pkIndexId;
            this.zone = zone;
        }
    }
}
