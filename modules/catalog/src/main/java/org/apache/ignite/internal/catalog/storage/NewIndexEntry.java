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
import java.util.Objects;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.catalog.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.serialization.EntrySerializationType;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Describes addition of a new index.
 */
public class NewIndexEntry implements UpdateEntry, Fireable {
    public static final CatalogObjectSerializer<NewIndexEntry> SERIALIZER = new NewIndexEntrySerializer();

    private final CatalogIndexDescriptor descriptor;

    private final String schemaName;

    /**
     * Constructs the object.
     *
     * @param descriptor A descriptor of an index to add.
     * @param schemaName Schema name.
     */
    public NewIndexEntry(CatalogIndexDescriptor descriptor, String schemaName) {
        this.descriptor = descriptor;
        this.schemaName = schemaName;
    }

    /** Gets descriptor of an index to add. */
    public CatalogIndexDescriptor descriptor() {
        return descriptor;
    }

    @Override
    public int typeId() {
        return EntrySerializationType.NEW_INDEX.id();
    }

    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.INDEX_CREATE;
    }

    @Override
    public CatalogEventParameters createEventParameters(long causalityToken, int catalogVersion) {
        return new CreateIndexEventParameters(causalityToken, catalogVersion, descriptor);
    }

    @Override
    public Catalog applyUpdate(Catalog catalog, long causalityToken) {
        CatalogSchemaDescriptor schema = Objects.requireNonNull(catalog.schema(schemaName));

        descriptor.updateToken(causalityToken);

        return new Catalog(
                catalog.version(),
                catalog.time(),
                catalog.objectIdGenState(),
                catalog.zones(),
                CatalogUtils.replaceSchema(new CatalogSchemaDescriptor(
                        schema.id(),
                        schema.name(),
                        schema.tables(),
                        ArrayUtils.concat(schema.indexes(), descriptor),
                        schema.systemViews(),
                        causalityToken
                ), catalog.schemas())
        );
    }

    @Override
    public String toString() {
        return S.toString(this);
    }

    /**
     * Serializer for {@link NewIndexEntry}.
     */
    private static class NewIndexEntrySerializer implements CatalogObjectSerializer<NewIndexEntry> {
        @Override
        public NewIndexEntry readFrom(int version, IgniteDataInput input) throws IOException {
            String schemaName = input.readUTF();
            CatalogIndexDescriptor descriptor = CatalogIndexDescriptor.SERIALIZER.readFrom(version, input);

            return new NewIndexEntry(descriptor, schemaName);
        }

        @Override
        public void writeTo(NewIndexEntry entry, int version, IgniteDataOutput output) throws IOException {
            output.writeUTF(entry.schemaName);
            CatalogIndexDescriptor.SERIALIZER.writeTo(entry.descriptor(), version, output);
        }
    }
}
