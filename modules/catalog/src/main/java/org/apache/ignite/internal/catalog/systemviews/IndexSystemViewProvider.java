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

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor.CatalogIndexDescriptorType.HASH;
import static org.apache.ignite.internal.type.NativeTypes.BOOLEAN;
import static org.apache.ignite.internal.type.NativeTypes.INT32;
import static org.apache.ignite.internal.type.NativeTypes.STRING;

import java.util.List;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogSystemViewProvider;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViews;
import org.apache.ignite.internal.util.SubscriptionUtils;

/**
 * Exposes information on index objects.
 *
 * <ul>
 *     <li>INDEXES system view</li>
 * </ul>
 */
public class IndexSystemViewProvider implements CatalogSystemViewProvider {

    /** {@inheritDoc} */
    @Override
    public List<SystemView<?>> getView(Supplier<Catalog> catalogSupplier) {
        Iterable<CatalogAwareDescriptor<CatalogIndexDescriptor>> viewData = () -> {
            Catalog catalog = catalogSupplier.get();

            return catalog.indexes().stream()
                    .filter(index -> index.status().isAlive())
                    .map(index -> new CatalogAwareDescriptor<>(index, catalog))
                    .iterator();
        };

        SystemView<?> indexView = SystemViews.<CatalogAwareDescriptor<CatalogIndexDescriptor>>clusterViewBuilder()
                .name("INDEXES")
                .addColumn("INDEX_ID", INT32, entry -> entry.descriptor.id())
                .addColumn("INDEX_NAME", STRING, entry -> entry.descriptor.name())
                .addColumn("TABLE_ID", INT32, entry -> entry.descriptor.tableId())
                .addColumn("TABLE_NAME", STRING, entry -> getTableDescriptor(entry).name())
                .addColumn("SCHEMA_ID", INT32, IndexSystemViewProvider::getSchemaId)
                .addColumn("SCHEMA_NAME", STRING, entry -> entry.catalog.schema(getSchemaId(entry)).name())
                .addColumn("TYPE", STRING, entry -> entry.descriptor.indexType().name())
                .addColumn("IS_UNIQUE", BOOLEAN, entry -> entry.descriptor.unique())
                .addColumn("COLUMNS", STRING, IndexSystemViewProvider::getColumnsString)
                .addColumn("STATUS", STRING, entry -> entry.descriptor.status().name())
                .dataProvider(SubscriptionUtils.fromIterable(viewData))
                .build();

        return List.of(indexView);
    }

    private static CatalogTableDescriptor getTableDescriptor(CatalogAwareDescriptor<CatalogIndexDescriptor> entry) {
        return entry.catalog.table(entry.descriptor.tableId());
    }

    private static String getColumnsString(CatalogAwareDescriptor<CatalogIndexDescriptor> entry) {
        return entry.descriptor.indexType() == HASH
                ? String.join(", ", ((CatalogHashIndexDescriptor) entry.descriptor).columns())
                : ((CatalogSortedIndexDescriptor) entry.descriptor)
                        .columns()
                        .stream()
                        .map(column -> column.name() + (column.collation().asc() ? " ASC" : " DESC"))
                        .collect(joining(", "));
    }

    private static int getSchemaId(CatalogAwareDescriptor<CatalogIndexDescriptor> entry) {
        return getTableDescriptor(entry).schemaId();
    }
}
