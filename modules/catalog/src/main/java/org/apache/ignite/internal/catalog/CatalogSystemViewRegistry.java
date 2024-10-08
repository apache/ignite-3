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

package org.apache.ignite.internal.catalog;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.systemviews.IndexSystemViewProvider;
import org.apache.ignite.internal.catalog.systemviews.SystemViewViewProvider;
import org.apache.ignite.internal.catalog.systemviews.TablesSystemViewProvider;
import org.apache.ignite.internal.catalog.systemviews.ZonesSystemViewProvider;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViewProvider;

/** Implementation of {@link SystemViewProvider} for the catalog. */
public class CatalogSystemViewRegistry implements SystemViewProvider {

    private final List<CatalogSystemViewProvider> providers;

    private final Supplier<Catalog> catalogSupplier;

    /** Constructor. */
    public CatalogSystemViewRegistry(Supplier<Catalog> catalogSupplier) {
        this.catalogSupplier = catalogSupplier;

        providers = List.of(
                new SystemViewViewProvider(),
                new IndexSystemViewProvider(),
                new ZonesSystemViewProvider(),
                new TablesSystemViewProvider()
        );
    }

    /** {@inheritDoc} */
    @Override
    public List<SystemView<?>> systemViews() {
        return providers.stream().flatMap(p -> p.getView(catalogSupplier).stream()).collect(Collectors.toList());
    }
}
