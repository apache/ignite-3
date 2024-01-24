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
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.replaceSchema;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.replaceTable;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.schemaOrThrow;

import java.io.IOException;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.AlterColumnEventParameters;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.serialization.EntrySerializationType;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Describes a column replacement.
 */
public class AlterColumnEntry implements UpdateEntry, Fireable {
    public static final CatalogObjectSerializer<AlterColumnEntry> SERIALIZER = new AlterColumnEntrySerializer();

    private final int tableId;

    private final CatalogTableColumnDescriptor column;

    private final String schemaName;

    /**
     * Constructs the object.
     *
     * @param tableId An id the table to be modified.
     * @param column A modified descriptor of the column to be replaced.
     * @param schemaName Schema name.
     */
    public AlterColumnEntry(int tableId, CatalogTableColumnDescriptor column, String schemaName) {
        this.tableId = tableId;
        this.column = column;
        this.schemaName = schemaName;
    }

    /** Returns a descriptor for the column to be replaced. */
    public CatalogTableColumnDescriptor descriptor() {
        return column;
    }

    @Override
    public int typeId() {
        return EntrySerializationType.ALTER_COLUMN.id();
    }

    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.TABLE_ALTER;
    }

    @Override
    public CatalogEventParameters createEventParameters(long causalityToken, int catalogVersion) {
        return new AlterColumnEventParameters(causalityToken, catalogVersion, tableId, column);
    }

    @Override
    public Catalog applyUpdate(Catalog catalog, long causalityToken) {
        CatalogSchemaDescriptor schema = schemaOrThrow(catalog, schemaName);

        CatalogTableDescriptor currentTableDescriptor = requireNonNull(catalog.table(tableId));

        CatalogTableDescriptor newTableDescriptor = currentTableDescriptor.newDescriptor(
                currentTableDescriptor.name(),
                currentTableDescriptor.tableVersion() + 1,
                currentTableDescriptor.columns().stream()
                        .map(source -> source.name().equals(column.name()) ? column : source)
                        .collect(toList()),
                causalityToken
        );

        return new Catalog(
                catalog.version(),
                catalog.time(),
                catalog.objectIdGenState(),
                catalog.zones(),
                replaceSchema(replaceTable(schema, newTableDescriptor), catalog.schemas())
        );
    }

    @Override
    public String toString() {
        return S.toString(this);
    }

    /**
     * Serializer for {@link AlterColumnEntry}.
     */
    private static class AlterColumnEntrySerializer implements CatalogObjectSerializer<AlterColumnEntry> {
        @Override
        public AlterColumnEntry readFrom(int version, IgniteDataInput input) throws IOException {
            CatalogTableColumnDescriptor descriptor = CatalogTableColumnDescriptor.SERIALIZER.readFrom(version, input);

            String schemaName = input.readUTF();
            int tableId = input.readInt();

            return new AlterColumnEntry(tableId, descriptor, schemaName);
        }

        @Override
        public void writeTo(AlterColumnEntry value, int version, IgniteDataOutput output) throws IOException {
            CatalogTableColumnDescriptor.SERIALIZER.writeTo(value.descriptor(), version, output);

            output.writeUTF(value.schemaName);
            output.writeInt(value.tableId);
        }
    }
}
