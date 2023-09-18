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
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.jetbrains.annotations.Nullable;

/**
 * This interface is needed to cope with the fact and for some time we have both configuration-defined and Catalog-defined
 * table IDs (which are usually not equal to each other even for the same table); it allows to hide the
 * translation logic as most of the code operates with config-defined table IDs, while {@link CatalogService} requires
 * its own table IDs.
 */
// TODO: IGNITE-20386 - remove after the switch to the Catalog.
@SuppressWarnings("InterfaceMayBeAnnotatedFunctional")
public interface CatalogTables {
    /**
     * Gets a table by a timestamp. Make sure there is enough metadata (see {@link SchemaSyncService}) before invoking this method.
     *
     * @param tableId ID of the table (before the switch to the Catalog, these are config-defined IDs).
     * @param timestamp Timestamp at which to look for a table.
     * @return Table descriptor or {@code null} if none was found.
     */
    @Nullable CatalogTableDescriptor table(int tableId, long timestamp);
}
