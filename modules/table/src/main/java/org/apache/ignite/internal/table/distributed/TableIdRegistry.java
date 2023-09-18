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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for configTableId->catalogTableId pairs.
 */
// TODO: IGNITE-20386 - remove after the switch to the Catalog.
public class TableIdRegistry implements TableIdTranslator {
    private final Map<Integer, Integer> configToCatalogIds = new ConcurrentHashMap<>();

    @Override
    public int configIdToCatalogId(int configTableId) {
        Integer catalogId = configToCatalogIds.get(configTableId);

        assert catalogId != null : "Translating " + configTableId + ", but nothing was found";

        return catalogId;
    }

    /**
     * Registers a mapping of one table ID to another (both belong to the same table).
     *
     * @param configTableId Config-defined table ID.
     * @param catalogTableId Catalog-defined table ID.
     */
    public void registerMapping(int configTableId, int catalogTableId) {
        configToCatalogIds.put(configTableId, catalogTableId);
    }
}
