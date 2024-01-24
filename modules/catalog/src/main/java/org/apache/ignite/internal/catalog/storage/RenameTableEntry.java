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
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.replaceTable;

import java.io.IOException;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.RenameTableEventParameters;
import org.apache.ignite.internal.catalog.serialization.CatalogEntrySerializer;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/** Entry representing a rename of a table. */
public class RenameTableEntry implements UpdateEntry, Fireable {
    public static CatalogEntrySerializer<RenameTableEntry> SERIALIZER = new RenameTableEntrySerializer();

    private final int tableId;

    private final String newTableName;

    public RenameTableEntry(int tableId, String newTableName) {
        this.tableId = tableId;
        this.newTableName = newTableName;
    }

    public int tableId() {
        return tableId;
    }

    public String newTableName() {
        return newTableName;
    }

    @Override
    public int typeId() {
        return UpdateEntryType.RENAME_TABLE.id();
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

        return new Catalog(
                catalog.version(),
                catalog.time(),
                catalog.objectIdGenState(),
                catalog.zones(),
                replaceSchema(replaceTable(schemaDescriptor, newTableDescriptor), catalog.schemas())
        );
    }

    @Override
    public String toString() {
        return S.toString(this);
    }

    /**
     * Serializer for {@link RenameTableEntry}.
     */
    private static class RenameTableEntrySerializer implements CatalogEntrySerializer<RenameTableEntry> {
        @Override
        public RenameTableEntry readFrom(int version, IgniteDataInput input) throws IOException {
            int tableId = input.readInt();
            String newTableName = input.readUTF();

            return new RenameTableEntry(tableId, newTableName);
        }

        @Override
        public void writeTo(RenameTableEntry entry, int version, IgniteDataOutput output) throws IOException {
            output.writeInt(entry.tableId());
            output.writeUTF(entry.newTableName());
        }
    }
}
