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
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.schemaOrThrow;

import java.io.IOException;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
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

    private final int schemaId;

    /**
     * Constructs the object.
     *
     * @param descriptor A descriptor of an index to add.
     * @param schemaId Schema id.
     */
    public NewIndexEntry(CatalogIndexDescriptor descriptor, int schemaId) {
        this.descriptor = descriptor;
        this.schemaId = schemaId;
    }

    /** Gets descriptor of an index to add. */
    public CatalogIndexDescriptor descriptor() {
        return descriptor;
    }

    @Override
    public int typeId() {
        return MarshallableEntryType.NEW_INDEX.id();
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
        CatalogSchemaDescriptor schema = schemaOrThrow(catalog, schemaId);

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
                ), catalog.schemas()),
                defaultZoneIdOpt(catalog)
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
        public NewIndexEntry readFrom(IgniteDataInput input) throws IOException {
            int schemaId = input.readInt();
            CatalogIndexDescriptor descriptor = CatalogSerializationUtils.IDX_SERIALIZER.readFrom(input);

            return new NewIndexEntry(descriptor, schemaId);
        }

        @Override
        public void writeTo(NewIndexEntry entry, IgniteDataOutput output) throws IOException {
            output.writeInt(entry.schemaId);
            CatalogSerializationUtils.IDX_SERIALIZER.writeTo(entry.descriptor(), output);
        }
    }
}
