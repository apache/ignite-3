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

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Describes deletion of a table.
 */
public class DropTableEntry implements UpdateEntry, Fireable {
    public static final CatalogObjectSerializer<DropTableEntry> SERIALIZER = new DropTableEntrySerializer();

    private final int tableId;

    private final String schemaName;

    /**
     * Constructs the object.
     *
     * @param tableId An id of a table to drop.
     * @param schemaName A schema name.
     */
    public DropTableEntry(int tableId, String schemaName) {
        this.tableId = tableId;
        this.schemaName = schemaName;
    }

    /** Returns an id of a table to drop. */
    public int tableId() {
        return tableId;
    }

    @Override
    public int typeId() {
        return MarshallableEntryType.DROP_TABLE.id();
    }

    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.TABLE_DROP;
    }

    @Override
    public CatalogEventParameters createEventParameters(long causalityToken, int catalogVersion) {
        return new DropTableEventParameters(causalityToken, catalogVersion, tableId);
    }

    @Override
    public Catalog applyUpdate(Catalog catalog, long causalityToken) {
        CatalogSchemaDescriptor schema = Objects.requireNonNull(catalog.schema(schemaName));

        return new Catalog(
                catalog.version(),
                catalog.time(),
                catalog.objectIdGenState(),
                catalog.zones(),
                CatalogUtils.replaceSchema(new CatalogSchemaDescriptor(
                        schema.id(),
                        schema.name(),
                        Arrays.stream(schema.tables()).filter(t -> t.id() != tableId).toArray(CatalogTableDescriptor[]::new),
                        schema.indexes(),
                        schema.systemViews(),
                        causalityToken
                ), catalog.schemas()),
                catalog.defaultZone().id()
        );
    }

    @Override
    public String toString() {
        return S.toString(this);
    }

    /**
     * Serializer for {@link DropTableEntry}.
     */
    private static class DropTableEntrySerializer implements CatalogObjectSerializer<DropTableEntry> {
        @Override
        public DropTableEntry readFrom(IgniteDataInput input) throws IOException {
            int tableId = input.readInt();
            String schemaName = input.readUTF();

            return new DropTableEntry(tableId, schemaName);
        }

        @Override
        public void writeTo(DropTableEntry entry, IgniteDataOutput out) throws IOException {
            out.writeInt(entry.tableId());
            out.writeUTF(entry.schemaName);
        }
    }
}
