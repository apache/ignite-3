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

package org.apache.ignite.internal.storage.index.impl;

import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.storage.index.CatalogIndexStatusSupplier;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation for tests, without taking into account the low watermark, if the index removed from the catalog, it is considered to be
 * destroyed.
 */
public class TestCatalogIndexStatusSupplier implements CatalogIndexStatusSupplier {
    private final CatalogService catalogService;

    /** Constructor. */
    public TestCatalogIndexStatusSupplier(CatalogService catalogService) {
        this.catalogService = catalogService;
    }

    @Override
    public @Nullable CatalogIndexStatus get(int indexId) {
        CatalogIndexDescriptor index = catalogService.index(indexId, catalogService.latestCatalogVersion());

        return index == null ? null : index.status();
    }
}
