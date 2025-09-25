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

package org.apache.ignite.internal.catalog.storage;

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.defaultZoneIdOpt;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.replaceSchema;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.replaceTable;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.schemaOrThrow;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.tableOrThrow;

import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;

/** Interface describing a particular change to a table within the {@link VersionedUpdate group}. */
interface UpdateTable extends UpdateEntry {
    @Override
    default Catalog applyUpdate(Catalog catalog, HybridTimestamp timestamp) {
        CatalogTableDescriptor table = tableOrThrow(catalog, tableId());
        CatalogSchemaDescriptor schema = schemaOrThrow(catalog, table.schemaId());

        CatalogTableDescriptor modifiedTable = newTableDescriptor(table)
                .tableVersion(newTableVersion(table))
                .timestamp(timestamp)
                .build();

        CatalogSchemaDescriptor modifiedSchemaDescriptor = replaceTable(schema, modifiedTable);

        return new Catalog(
                catalog.version(),
                catalog.time(),
                catalog.objectIdGenState(),
                catalog.zones(),
                replaceSchema(modifiedSchemaDescriptor, catalog.schemas()),
                defaultZoneIdOpt(catalog)
        );
    }

    /**
     * Creates updated version of {@link CatalogTableDescriptor}.
     * The table version and casuality token would be incremented automatically.
     *
     * @param table previous table descriptor definition.
     *
     * @return builder with the updated fields for this table descriptor, created with {@link CatalogTableDescriptor#copyBuilder()}
     **/
    CatalogTableDescriptor.Builder newTableDescriptor(CatalogTableDescriptor table);

    /** Returns table id for a table affected by an update table command. */
    int tableId();

    /** Returns a new table version for provided {@link CatalogTableDescriptor}. */
    default int newTableVersion(CatalogTableDescriptor table) {
        return table.tableVersion() + 1;
    }
}
