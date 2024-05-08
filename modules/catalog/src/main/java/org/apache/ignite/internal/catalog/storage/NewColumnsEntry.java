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
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.defaultZoneIdOpt;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.replaceSchema;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.replaceTable;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.schemaOrThrow;
import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.readList;
import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.writeList;

import java.io.IOException;
import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.AddColumnEventParameters;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Describes addition of new columns.
 */
public class NewColumnsEntry implements UpdateEntry, Fireable {
    public static final CatalogObjectSerializer<NewColumnsEntry> SERIALIZER = new NewColumnsEntrySerializer();

    private final int tableId;
    private final List<CatalogTableColumnDescriptor> descriptors;
    private final int schemaId;

    /**
     * Constructs the object.
     *
     * @param tableId Table id.
     * @param descriptors Descriptors of columns to add.
     */
    public NewColumnsEntry(int tableId, List<CatalogTableColumnDescriptor> descriptors, int schemaId) {
        this.tableId = tableId;
        this.descriptors = descriptors;
        this.schemaId = schemaId;
    }

    /** Returns table id. */
    public int tableId() {
        return tableId;
    }

    /** Returns descriptors of columns to add. */
    public List<CatalogTableColumnDescriptor> descriptors() {
        return descriptors;
    }

    @Override
    public int typeId() {
        return MarshallableEntryType.NEW_COLUMN.id();
    }

    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.TABLE_ALTER;
    }

    @Override
    public CatalogEventParameters createEventParameters(long causalityToken, int catalogVersion) {
        return new AddColumnEventParameters(causalityToken, catalogVersion, tableId, descriptors);
    }

    @Override
    public Catalog applyUpdate(Catalog catalog, long causalityToken) {
        CatalogSchemaDescriptor schema = schemaOrThrow(catalog, schemaId);

        CatalogTableDescriptor currentTableDescriptor = requireNonNull(catalog.table(tableId));

        CatalogTableDescriptor newTableDescriptor = currentTableDescriptor.newDescriptor(
                currentTableDescriptor.name(),
                currentTableDescriptor.tableVersion() + 1,
                CollectionUtils.concat(currentTableDescriptor.columns(), descriptors),
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
     * Serializer for {@link NewColumnsEntry}.
     */
    private static class NewColumnsEntrySerializer implements CatalogObjectSerializer<NewColumnsEntry> {
        @Override
        public NewColumnsEntry readFrom(IgniteDataInput in) throws IOException {
            List<CatalogTableColumnDescriptor> columns = readList(CatalogTableColumnDescriptor.SERIALIZER, in);
            int tableId = in.readInt();
            int schemaId = in.readInt();

            return new NewColumnsEntry(tableId, columns, schemaId);
        }

        @Override
        public void writeTo(NewColumnsEntry entry, IgniteDataOutput out) throws IOException {
            writeList(entry.descriptors(), CatalogTableColumnDescriptor.SERIALIZER, out);
            out.writeInt(entry.tableId());
            out.writeInt(entry.schemaId);
        }
    }
}
