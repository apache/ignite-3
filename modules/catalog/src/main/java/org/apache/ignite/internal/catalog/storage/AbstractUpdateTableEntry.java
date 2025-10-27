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

/**
 * This class declares an abstract method for creating a new table descriptor and
 * provides default implementation for applying the table descriptor changes to a catalog.
 */
abstract class AbstractUpdateTableEntry implements UpdateEntry {
    @Override
    public final Catalog applyUpdate(Catalog catalog, HybridTimestamp timestamp) {
        CatalogTableDescriptor table = tableOrThrow(catalog, tableId());
        CatalogSchemaDescriptor schema = schemaOrThrow(catalog, table.schemaId());

        CatalogTableDescriptor modifiedTable = newTableDescriptor(table)
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
     * Creates a {@link CatalogTableDescriptor.Builder} with modifications of this table.
     * The timestamp of this table will be updated automatically.
     *
     * @param table previous table descriptor definition.
     *
     * @return builder with the updated fields for this table descriptor, created with {@link CatalogTableDescriptor#copyBuilder()}
     **/
    abstract CatalogTableDescriptor.Builder newTableDescriptor(CatalogTableDescriptor table);

    /** Returns table id for a table affected by an update table command. */
    abstract int tableId();

    /** Returns the next schema version for provided {@link CatalogTableDescriptor}. */
    static int nextSchemaVersion(CatalogTableDescriptor table) {
        return table.latestSchemaVersion() + 1;
    }
}
