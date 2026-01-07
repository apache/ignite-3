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

package org.apache.ignite.internal.app;

import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.jetbrains.annotations.Nullable;

/**
 * Component that rectifies the low watermark on node startup if it is lower than the earliest catalog timestamp.
 */
class LowWatermarkRectifier implements IgniteComponent {
    private final LowWatermark lowWatermark;
    private final CatalogService catalogService;

    LowWatermarkRectifier(LowWatermark lowWatermark, CatalogService catalogService) {
        this.lowWatermark = lowWatermark;
        this.catalogService = catalogService;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        HybridTimestamp earliestCatalogTimestamp = hybridTimestamp(catalogService.earliestCatalog().time());
        @Nullable HybridTimestamp lwm = lowWatermark.getLowWatermark();

        if (lwm != null && lwm.compareTo(earliestCatalogTimestamp) < 0) {
            lowWatermark.setLowWatermarkOnRecovery(earliestCatalogTimestamp);
        }

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        return nullCompletedFuture();
    }
}
