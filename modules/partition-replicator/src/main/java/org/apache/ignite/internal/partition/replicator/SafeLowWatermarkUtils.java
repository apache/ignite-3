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

package org.apache.ignite.internal.partition.replicator;

import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;

import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class for safe low watermark operations.
 */
public class SafeLowWatermarkUtils {
    /**
     * Returns a catalog-safe low watermark (that is, a timestamp that can be used to access the catalog).
     * If the low watermark is @{code null}, @{code null} is returned. If the low watermark is less than the earliest catalog timestamp,
     * the earliest catalog timestamp is returned. Otherwise, the original low watermark is returned.
     *
     * @param watermark The low watermark.
     * @param catalogService The catalog service.
     * @return A catalog-safe low watermark or @{code null}.
     */
    public static @Nullable HybridTimestamp catalogSafeLowWatermark(LowWatermark watermark, CatalogService catalogService) {
        HybridTimestamp lwmTimestamp = watermark.getLowWatermark();

        if (lwmTimestamp == null) {
            return null;
        }

        HybridTimestamp earliestCatalogTimestamp = hybridTimestamp(catalogService.earliestCatalog().time());

        if (lwmTimestamp.compareTo(earliestCatalogTimestamp) < 0) {
            return earliestCatalogTimestamp;
        } else {
            return lwmTimestamp;
        }
    }
}
