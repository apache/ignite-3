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

import static org.apache.ignite.internal.catalog.storage.AbstractChangeIndexStatusEntry.schemaByIndexId;

import java.io.IOException;
import java.util.Arrays;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.RemoveIndexEventParameters;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Describes removal of an index from the Catalog (not the same as dropping it [that just initates the drop sequence]).
 */
public class RemoveIndexEntry implements UpdateEntry, Fireable {
    public static final CatalogObjectSerializer<RemoveIndexEntry> SERIALIZER = new RemoveIndexEntrySerializer();

    private final int indexId;

    /**
     * Constructs the object.
     *
     * @param indexId An id of an index to drop.
     */
    public RemoveIndexEntry(int indexId) {
        this.indexId = indexId;
    }

    @Override
    public int typeId() {
        return MarshallableEntryType.REMOVE_INDEX.id();
    }

    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.INDEX_REMOVED;
    }

    @Override
    public CatalogEventParameters createEventParameters(long causalityToken, int catalogVersion) {
        return new RemoveIndexEventParameters(causalityToken, catalogVersion, indexId);
    }

    @Override
    public Catalog applyUpdate(Catalog catalog, long causalityToken) {
        CatalogSchemaDescriptor schema = schemaByIndexId(catalog, indexId);

        return new Catalog(
                catalog.version(),
                catalog.time(),
                catalog.objectIdGenState(),
                catalog.zones(),
                CatalogUtils.replaceSchema(new CatalogSchemaDescriptor(
                        schema.id(),
                        schema.name(),
                        schema.tables(),
                        Arrays.stream(schema.indexes()).filter(t -> t.id() != indexId).toArray(CatalogIndexDescriptor[]::new),
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
     * Serializer for {@link RemoveIndexEntry}.
     */
    private static class RemoveIndexEntrySerializer implements CatalogObjectSerializer<RemoveIndexEntry> {
        @Override
        public RemoveIndexEntry readFrom(IgniteDataInput input) throws IOException {
            int indexId = input.readInt();

            return new RemoveIndexEntry(indexId);
        }

        @Override
        public void writeTo(RemoveIndexEntry entry, IgniteDataOutput out) throws IOException {
            out.writeInt(entry.indexId);
        }
    }
}
