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

import static java.util.Objects.requireNonNull;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.replaceSchema;

import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.RenameTableEventParameters;

/** Entry representing a rename of a table. */
public class RenameTableEntry implements UpdateEntry, Fireable {
    private static final long serialVersionUID = 4841281816270322196L;

    private final int tableId;

    private final String newTableName;

    public RenameTableEntry(int tableId, String newTableName) {
        this.tableId = tableId;
        this.newTableName = newTableName;
    }

    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.TABLE_ALTER;
    }

    @Override
    public CatalogEventParameters createEventParameters(long causalityToken, int catalogVersion) {
        return new RenameTableEventParameters(causalityToken, catalogVersion, tableId, newTableName);
    }

    @Override
    public Catalog applyUpdate(Catalog catalog, long causalityToken) {
        CatalogTableDescriptor tableDescriptor = requireNonNull(catalog.table(tableId));

        CatalogSchemaDescriptor schemaDescriptor = requireNonNull(catalog.schema(tableDescriptor.schemaId()));

        CatalogTableDescriptor newTableDescriptor = tableDescriptor.newDescriptor(
                newTableName,
                tableDescriptor.tableVersion() + 1,
                tableDescriptor.columns(),
                causalityToken
        );

        CatalogSchemaDescriptor newSchemaDescriptor = replaceTableDescriptor(schemaDescriptor, newTableDescriptor, causalityToken);

        return new Catalog(
                catalog.version(),
                catalog.time(),
                catalog.objectIdGenState(),
                catalog.zones(),
                replaceSchema(newSchemaDescriptor, catalog.schemas())
        );
    }

    private static CatalogSchemaDescriptor replaceTableDescriptor(
            CatalogSchemaDescriptor schemaDescriptor,
            CatalogTableDescriptor newTableDescriptor,
            long causalityToken
    ) {
        CatalogTableDescriptor[] tableDescriptors = schemaDescriptor.tables().clone();

        tableDescriptors[indexOf(tableDescriptors, newTableDescriptor)] = newTableDescriptor;

        return new CatalogSchemaDescriptor(
                schemaDescriptor.id(),
                schemaDescriptor.name(),
                tableDescriptors,
                schemaDescriptor.indexes(),
                schemaDescriptor.systemViews(),
                causalityToken
        );
    }

    private static int indexOf(CatalogTableDescriptor[] tableDescriptors, CatalogTableDescriptor newTableDescriptor) {
        for (int i = 0; i < tableDescriptors.length; i++) {
            if (tableDescriptors[i].id() == newTableDescriptor.id()) {
                return i;
            }
        }

        throw new IllegalStateException(String.format(
                "Table with ID %d has not been found in schema with ID %d", newTableDescriptor.id(), newTableDescriptor.schemaId()
        ));
    }
}
