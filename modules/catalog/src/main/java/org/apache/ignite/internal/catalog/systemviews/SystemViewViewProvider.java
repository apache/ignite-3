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
import static org.apache.ignite.internal.type.NativeTypes.stringOf;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogSystemViewProvider;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViews;
import org.apache.ignite.internal.util.SubscriptionUtils;

/**
 * Exposes information on system views.
 *
 * <ul>
 *     <li>SYSTEM_VIEWS - available system views</li>
 *     <li>SYSTEM_VIEW_COLUMNS - columns of available system view columns.</li>
 * </ul>
 */
public class SystemViewViewProvider implements CatalogSystemViewProvider {

    private static final int SYSTEM_VIEW_STRING_COLUMN_LENGTH = Short.MAX_VALUE;

    /** {@inheritDoc} */
    @Override
    public List<SystemView<?>> getView(Supplier<Catalog> catalogSupplier) {
        SystemView<?> systemViewsView = getSystemViewView(catalogSupplier);
        SystemView<?> systemViewColumnsView = getSystemViewColumnsView(catalogSupplier);

        return List.of(systemViewsView, systemViewColumnsView);
    }

    private static SystemView<?> getSystemViewView(Supplier<Catalog> catalogSupplier) {
        Iterable<SchemaAwareDescriptor<CatalogSystemViewDescriptor>> viewData = () -> {
            Catalog catalog = catalogSupplier.get();

            return catalog.schemas().stream()
                    .flatMap(schema -> Arrays.stream(schema.systemViews())
                            .map(viewDescriptor -> new SchemaAwareDescriptor<>(viewDescriptor, schema.name()))
                    )
                    .iterator();
        };

        Publisher<SchemaAwareDescriptor<CatalogSystemViewDescriptor>> viewDataPublisher = SubscriptionUtils.fromIterable(viewData);

        return SystemViews.<SchemaAwareDescriptor<CatalogSystemViewDescriptor>>clusterViewBuilder()
                .name("SYSTEM_VIEWS")
                .addColumn("ID", INT32, entry -> entry.descriptor.id())
                .addColumn("SCHEMA", stringOf(SYSTEM_VIEW_STRING_COLUMN_LENGTH), entry -> entry.schema)
                .addColumn("NAME", stringOf(SYSTEM_VIEW_STRING_COLUMN_LENGTH), entry -> entry.descriptor.name())
                .addColumn("TYPE", stringOf(SYSTEM_VIEW_STRING_COLUMN_LENGTH), entry -> entry.descriptor.systemViewType().name())
                .dataProvider(viewDataPublisher)
                .build();
    }

    private static SystemView<?> getSystemViewColumnsView(Supplier<Catalog> catalogSupplier) {
        Iterable<ParentIdAwareDescriptor<CatalogTableColumnDescriptor>> viewData = () -> {
            Catalog catalog = catalogSupplier.get();

            return catalog.schemas().stream()
                    .flatMap(schema -> Arrays.stream(schema.systemViews()))
                    .flatMap(viewDescriptor -> viewDescriptor.columns().stream()
                            .map(columnDescriptor -> new ParentIdAwareDescriptor<>(columnDescriptor, viewDescriptor.id()))
                    )
                    .iterator();
        };

        Publisher<ParentIdAwareDescriptor<CatalogTableColumnDescriptor>> viewDataPublisher = SubscriptionUtils.fromIterable(viewData);

        return SystemViews.<ParentIdAwareDescriptor<CatalogTableColumnDescriptor>>clusterViewBuilder()
                .name("SYSTEM_VIEW_COLUMNS")
                .addColumn("VIEW_ID", INT32, entry -> entry.id)
                .addColumn("NAME", stringOf(SYSTEM_VIEW_STRING_COLUMN_LENGTH), entry -> entry.descriptor.name())
                .addColumn("TYPE", stringOf(SYSTEM_VIEW_STRING_COLUMN_LENGTH), entry -> entry.descriptor.type().name())
                .addColumn("NULLABLE", BOOLEAN, entry -> entry.descriptor.nullable())
                .addColumn("PRECISION", INT32, entry -> entry.descriptor.precision())
                .addColumn("SCALE", INT32, entry -> entry.descriptor.scale())
                .addColumn("LENGTH", INT32, entry -> entry.descriptor.length())
                .dataProvider(viewDataPublisher)
                .build();
    }
}
