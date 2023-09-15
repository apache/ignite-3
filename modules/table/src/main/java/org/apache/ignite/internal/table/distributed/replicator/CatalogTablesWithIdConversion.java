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

package org.apache.ignite.internal.table.distributed.replicator;

import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.table.distributed.TableIdTranslator;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation which accepts configuration-defined table IDs, converts them under the hood to Catalog-defined ones
 * and uses them to access table information in the {@link CatalogService}.
 */
// TODO: IGNITE-20386 - replace all usages with DirectCatalogTables and remove after the switch to the Catalog.
public class CatalogTablesWithIdConversion implements CatalogTables {
    private final CatalogService catalogService;

    private final TableIdTranslator tableIdTranslator;

    /** Constructor. */
    public CatalogTablesWithIdConversion(CatalogService catalogService, TableIdTranslator tableIdTranslator) {
        this.catalogService = catalogService;
        this.tableIdTranslator = tableIdTranslator;
    }

    @Override
    public @Nullable CatalogTableDescriptor table(int tableId, long timestamp) {
        int catalogTableId = tableIdTranslator.configIdToCatalogId(tableId);

        return catalogService.table(catalogTableId, timestamp);
    }
}
