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

package org.apache.ignite.internal.table.distributed;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.storage.index.CatalogIndexStatusSupplier;
import org.jetbrains.annotations.Nullable;

/** Implementation with caching of index statuses. */
// TODO: IGNITE-21608 Don’t forget to check if, as a result of fixing issues with catalog compaction, it will be possible to use a different
//  implementation
// TODO: IGNITE-22039 реализовать и протестировать
public class CatalogIndexStatusSupplierImpl implements CatalogIndexStatusSupplier, IgniteComponent {
    private final CatalogService catalogService;

    private final LowWatermark lowWatermark;

    /** Constructor. */
    public CatalogIndexStatusSupplierImpl(CatalogService catalogService, LowWatermark lowWatermark) {
        this.catalogService = catalogService;
        this.lowWatermark = lowWatermark;
    }

    @Override
    public CompletableFuture<Void> start() {
        return null;
    }

    @Override
    public void stop() throws Exception {

    }

    @Override
    public @Nullable CatalogIndexStatus get(int indexId) {
        return null;
    }
}
