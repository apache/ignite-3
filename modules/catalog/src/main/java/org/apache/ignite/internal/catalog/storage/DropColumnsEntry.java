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
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.defaultZoneIdOpt;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.replaceSchema;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.replaceTable;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.schemaOrThrow;
import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.writeStringCollection;
import static org.apache.ignite.internal.util.IgniteUtils.capacity;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.DropColumnEventParameters;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Describes dropping of columns.
 */
public class DropColumnsEntry implements UpdateEntry, Fireable {
    public static final CatalogObjectSerializer<DropColumnsEntry> SERIALIZER = new DropColumnEntrySerializer();

    private final int tableId;
    private final Set<String> columns;
    private final String schemaName;

    /**
     * Constructs the object.
     *
     * @param tableId Table id.
     * @param columns Names of columns to drop.
     * @param schemaName Schema name.
     */
    public DropColumnsEntry(int tableId, Set<String> columns, String schemaName) {
        this.tableId = tableId;
        this.columns = columns;
        this.schemaName = schemaName;
    }

    /** Returns table id. */
    public int tableId() {
        return tableId;
    }

    /** Returns name of columns to drop. */
    public Set<String> columns() {
        return columns;
    }

    @Override
    public int typeId() {
        return MarshallableEntryType.DROP_COLUMN.id();
    }

    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.TABLE_ALTER;
    }

    @Override
    public CatalogEventParameters createEventParameters(long causalityToken, int catalogVersion) {
        return new DropColumnEventParameters(causalityToken, catalogVersion, tableId, columns);
    }

    @Override
    public Catalog applyUpdate(Catalog catalog, long causalityToken) {
        CatalogSchemaDescriptor schema = schemaOrThrow(catalog, schemaName);

        CatalogTableDescriptor currentTableDescriptor = requireNonNull(catalog.table(tableId));

        CatalogTableDescriptor newTableDescriptor = currentTableDescriptor.newDescriptor(
                currentTableDescriptor.name(),
                currentTableDescriptor.tableVersion() + 1,
                currentTableDescriptor.columns().stream()
                        .filter(col -> !columns.contains(col.name()))
                        .collect(toList()),
                causalityToken,
                currentTableDescriptor.storageProfile()
        );

        return new Catalog(
                catalog.version(),
                catalog.time(),
                catalog.objectIdGenState(),
                catalog.zones(),
                replaceSchema(replaceTable(schema, newTableDescriptor), catalog.schemas()),
                defaultZoneIdOpt(catalog)
        );
    }

    @Override
    public String toString() {
        return S.toString(this);
    }

    /**
     * Serializer for {@link DropColumnsEntry}.
     */
    private static class DropColumnEntrySerializer implements CatalogObjectSerializer<DropColumnsEntry> {
        @Override
        public DropColumnsEntry readFrom(IgniteDataInput input) throws IOException {
            String schemaName = input.readUTF();
            int tableId = input.readInt();
            Set<String> columns = CatalogSerializationUtils.readStringCollection(input, size -> new HashSet<>(capacity(size)));

            return new DropColumnsEntry(tableId, columns, schemaName);
        }

        @Override
        public void writeTo(DropColumnsEntry object, IgniteDataOutput output) throws IOException {
            output.writeUTF(object.schemaName);
            output.writeInt(object.tableId());
            writeStringCollection(object.columns(), output);
        }
    }
}
