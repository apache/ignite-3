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

package org.apache.ignite.internal.table.distributed.schema;

import org.apache.ignite.internal.catalog.CatalogService;

/**
 * Logic that allows to determine whether the logcal Catalog version is sufficient.
 */
public class CatalogVersionSufficiency {
    private CatalogVersionSufficiency() {
        // Deny instantiation.
    }

    /**
     * Determines whether the local Catalog version is sufficient.
     *
     * @param requiredCatalogVersion Minimal catalog version that is required to present.
     * @param catalogService Catalog service.
     * @return {@code true} iff the local Catalog version is sufficient.
     */
    public static boolean isMetadataAvailableFor(int requiredCatalogVersion, CatalogService catalogService) {
        return requiredCatalogVersion <= catalogService.latestCatalogVersion();
    }
}
