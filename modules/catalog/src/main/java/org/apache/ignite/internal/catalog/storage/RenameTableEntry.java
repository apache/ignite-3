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

import java.io.IOException;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.RenameTableEventParameters;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/** Entry representing a rename of a table. */
public class RenameTableEntry implements UpdateEntry, Fireable {
    public static final CatalogObjectSerializer<RenameTableEntry> SERIALIZER = new RenameTableEntrySerializer();

    private final int tableId;

    private final String newTableName;

    public RenameTableEntry(int tableId, String newTableName) {
        this.tableId = tableId;
        this.newTableName = newTableName;
    }

    @Override
    public int typeId() {
        return MarshallableEntryType.RENAME_TABLE.id();
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
        CatalogTableDescriptor table = tableOrThrow(catalog, tableId);
        CatalogSchemaDescriptor schema = schemaOrThrow(catalog, table.schemaId());

        CatalogTableDescriptor newTable = table.newDescriptor(
                newTableName,
                table.tableVersion() + 1,
                table.columns(),
                causalityToken,
                table.storageProfile()
        );

        return new Catalog(
                catalog.version(),
                catalog.time(),
                catalog.objectIdGenState(),
                catalog.zones(),
                replaceSchema(replaceTable(schema, newTable), catalog.schemas()),
                defaultZoneIdOpt(catalog)
        );
    }

    /**
     * Serializer for {@link RenameTableEntry}.
     */
    private static class RenameTableEntrySerializer implements CatalogObjectSerializer<RenameTableEntry> {
        @Override
        public RenameTableEntry readFrom(IgniteDataInput input) throws IOException {
            int tableId = input.readInt();
            String newTableName = input.readUTF();

            return new RenameTableEntry(tableId, newTableName);
        }

        @Override
        public void writeTo(RenameTableEntry entry, IgniteDataOutput output) throws IOException {
            output.writeInt(entry.tableId);
            output.writeUTF(entry.newTableName);
        }
    }
}
