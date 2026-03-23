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

import static org.apache.ignite.internal.type.NativeTypes.INT32;
import static org.apache.ignite.internal.type.NativeTypes.STRING;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogSystemViewProvider;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.systemview.api.ClusterSystemView;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViews;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Exposes information on index columns.
 *
 * <ul>
 *     <li>INDEX_COLUMNS system view</li>
 * </ul>
 */
public class IndexColumnsSystemViewProvider implements CatalogSystemViewProvider {
    /** {@inheritDoc} */
    @Override
    public List<SystemView<?>> getView(Supplier<Catalog> catalogSupplier) {
        Iterable<IndexColumn> indexColumns = () -> {
            Catalog catalog = catalogSupplier.get();

            return catalog.schemas().stream()
                    .flatMap(schema -> Arrays.stream(schema.indexes()).flatMap(index -> {
                        CatalogTableDescriptor table = catalog.table(index.tableId());
                        if (table == null) {
                            return Stream.empty();
                        } else {
                            return Stream.of(new IndexColumnContainer(schema, table, index));
                        }
                    }))
                    .flatMap(container -> {
                        switch (container.index.indexType()) {
                            case HASH:
                                return createHashIndexColumns(container, (CatalogHashIndexDescriptor) container.index);
                            case SORTED:
                                return createSortedIndexColumns(container, (CatalogSortedIndexDescriptor) container.index);
                            default:
                                return Stream.empty();
                        }
                    }).iterator();
        };

        Publisher<IndexColumn> viewDataPublisher = SubscriptionUtils.fromIterable(indexColumns);

        ClusterSystemView<IndexColumn> systemView = SystemViews.<IndexColumn>clusterViewBuilder()
                .name("INDEX_COLUMNS")
                .addColumn("SCHEMA_ID", INT32, col -> col.container.schemaId)
                .addColumn("SCHEMA_NAME", STRING, col -> col.container.schemaName)
                .addColumn("TABLE_ID", INT32, col -> col.container.tableId)
                .addColumn("TABLE_NAME", STRING, col -> col.container.tableName)
                .addColumn("INDEX_ID", INT32, col -> col.container.indexId)
                .addColumn("INDEX_NAME", STRING, col -> col.container.indexName)
                .addColumn("COLUMN_NAME", STRING, col -> col.columnName)
                .addColumn("COLUMN_ORDINAL", INT32, col -> col.columnOrdinal)
                .addColumn("COLUMN_COLLATION", STRING, col -> col.columnCollation)
                .dataProvider(viewDataPublisher)
                .build();

        return List.of(systemView);
    }

    private static Stream<IndexColumn> createSortedIndexColumns(
            IndexColumnContainer container,
            CatalogSortedIndexDescriptor index
    ) {
        List<IndexColumn> columns = new ArrayList<>();

        for (CatalogIndexColumnDescriptor indexColumn : index.columns()) {
            CatalogTableColumnDescriptor column = container.table.columnById(indexColumn.columnId());

            assert column != null;

            columns.add(new IndexColumn(container, column.name(), columns.size(), indexColumn.collation()));
        }

        return columns.stream();
    }

    private static Stream<IndexColumn> createHashIndexColumns(
            IndexColumnContainer container,
            CatalogHashIndexDescriptor index
    ) {
        List<IndexColumn> columns = new ArrayList<>();

        for (int columnId : index.columnIds()) {
            CatalogTableColumnDescriptor column = container.table.columnById(columnId);

            assert column != null;

            columns.add(new IndexColumn(container, column.name(), columns.size()));
        }

        return columns.stream();
    }

    private static class IndexColumnContainer {
        private final int schemaId;

        private final String schemaName;

        private final int tableId;

        private final String tableName;

        private final int indexId;

        private final String indexName;

        private final CatalogIndexDescriptor index;
        private final CatalogTableDescriptor table;

        IndexColumnContainer(
                CatalogSchemaDescriptor schema,
                CatalogTableDescriptor table,
                CatalogIndexDescriptor index
        ) {

            this.schemaId = schema.id();
            this.schemaName = schema.name();
            this.tableId = table.id();
            this.tableName = table.name();
            this.indexId = index.id();
            this.indexName = index.name();
            this.index = index;
            this.table = table;
        }
    }

    private static class IndexColumn {

        private final IndexColumnContainer container;

        private final String columnName;

        private final int columnOrdinal;

        private final @Nullable String columnCollation;

        IndexColumn(IndexColumnContainer container, String name, int ordinal) {
            this.container = container;
            this.columnName = name;
            this.columnOrdinal = ordinal;
            this.columnCollation = null;
        }

        IndexColumn(IndexColumnContainer container, String name, int ordinal, CatalogColumnCollation collation) {
            this.container = container;
            this.columnName = name;
            this.columnOrdinal = ordinal;
            this.columnCollation = collation.name();
        }
    }
}
