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

import java.util.List;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogSystemViewProvider;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViews;
import org.apache.ignite.internal.util.SubscriptionUtils;

/**
 * Exposes information on schemas.
 *
 * <ul>
 *     <li>SCHEMAS system view</li>
 * </ul>
 */
public class SchemasSystemViewProvider implements CatalogSystemViewProvider {

    /** {@inheritDoc} */
    @Override
    public List<SystemView<?>> getView(Supplier<Catalog> catalogSupplier) {
        return List.of(getSchemasSystemView(catalogSupplier));
    }

    private static SystemView<?> getSchemasSystemView(Supplier<Catalog> catalogSupplier) {
        Iterable<CatalogSchemaDescriptor> tablesData = () -> {
            Catalog catalog = catalogSupplier.get();
            return catalog.schemas().stream().iterator();
        };

        Publisher<CatalogSchemaDescriptor> viewDataPublisher = SubscriptionUtils.fromIterable(tablesData);

        return SystemViews.<CatalogSchemaDescriptor>clusterViewBuilder()
                .name("SCHEMAS")
                .addColumn("SCHEMA_ID", INT32, CatalogSchemaDescriptor::id)
                .addColumn("SCHEMA_NAME", STRING, CatalogObjectDescriptor::name)
                .dataProvider(viewDataPublisher)
                .build();
    }
}
