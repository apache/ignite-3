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
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Describes addition of a new table.
 */
public class NewTableEntry implements UpdateEntry, Fireable {
    public static final CatalogObjectSerializer<NewTableEntry> SERIALIZER = new NewTableEntrySerializer();

    @IgniteToStringInclude
    private final CatalogTableDescriptor descriptor;

    @IgniteToStringInclude
    private final String schemaName;

    /**
     * Constructs the object.
     *
     * @param descriptor A descriptor of a table to add.
     * @param schemaName A schema name.
     */
    public NewTableEntry(CatalogTableDescriptor descriptor, String schemaName) {
        this.descriptor = descriptor;
        this.schemaName = schemaName;
    }

    /** Returns descriptor of a table to add. */
    public CatalogTableDescriptor descriptor() {
        return descriptor;
    }

    @Override
    public int typeId() {
        return MarshallableEntryType.NEW_TABLE.id();
    }

    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.TABLE_CREATE;
    }

    @Override
    public CatalogEventParameters createEventParameters(long causalityToken, int catalogVersion) {
        return new CreateTableEventParameters(causalityToken, catalogVersion, descriptor);
    }

    @Override
    public Catalog applyUpdate(Catalog catalog, long causalityToken) {
        CatalogSchemaDescriptor schema = Objects.requireNonNull(catalog.schema(schemaName));

        descriptor.updateToken(causalityToken);

        List<CatalogSchemaDescriptor> schemas = CatalogUtils.replaceSchema(new CatalogSchemaDescriptor(
                schema.id(),
                schema.name(),
                ArrayUtils.concat(schema.tables(), descriptor),
                schema.indexes(),
                schema.systemViews(),
                causalityToken
        ), catalog.schemas());

        return new Catalog(
                catalog.version(),
                catalog.time(),
                catalog.objectIdGenState(),
                catalog.zones(),
                schemas
        );
    }

    @Override
    public String toString() {
        return S.toString(this);
    }

    /**
     * Serializer for {@link NewTableEntry}.
     */
    private static class NewTableEntrySerializer implements CatalogObjectSerializer<NewTableEntry> {
        @Override
        public NewTableEntry readFrom(IgniteDataInput input) throws IOException {
            CatalogTableDescriptor descriptor = CatalogTableDescriptor.SERIALIZER.readFrom(input);
            String schemaName = input.readUTF();

            return new NewTableEntry(descriptor, schemaName);
        }

        @Override
        public void writeTo(NewTableEntry entry, IgniteDataOutput output) throws IOException {
            CatalogTableDescriptor.SERIALIZER.writeTo(entry.descriptor(), output);
            output.writeUTF(entry.schemaName);
        }
    }
}
