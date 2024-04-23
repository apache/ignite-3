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
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateZoneEventParameters;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Describes addition of a new zone.
 */
public class NewZoneEntry implements UpdateEntry, Fireable {
    public static final CatalogObjectSerializer<NewZoneEntry> SERIALIZER = new NewZoneEntrySerializer();

    private final CatalogZoneDescriptor descriptor;

    /**
     * Constructs the object.
     *
     * @param descriptor A descriptor of a zone to add.
     */
    public NewZoneEntry(CatalogZoneDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    /** Returns descriptor of a zone to add. */
    public CatalogZoneDescriptor descriptor() {
        return descriptor;
    }

    @Override
    public int typeId() {
        return MarshallableEntryType.NEW_ZONE.id();
    }

    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.ZONE_CREATE;
    }

    @Override
    public CatalogEventParameters createEventParameters(long causalityToken, int catalogVersion) {
        return new CreateZoneEventParameters(causalityToken, catalogVersion, descriptor);
    }

    @Override
    public Catalog applyUpdate(Catalog catalog, long causalityToken) {
        descriptor.updateToken(causalityToken);

        int defaultZoneId = catalog.defaultZone() != null
                ? catalog.defaultZone().id()
                : descriptor.id();

        return new Catalog(
                catalog.version(),
                catalog.time(),
                catalog.objectIdGenState(),
                CollectionUtils.concat(catalog.zones(), List.of(descriptor)),
                catalog.schemas(),
                defaultZoneId
        );
    }

    @Override
    public String toString() {
        return S.toString(this);
    }

    /**
     * Serializer for {@link NewZoneEntry}.
     */
    private static class NewZoneEntrySerializer implements CatalogObjectSerializer<NewZoneEntry> {
        @Override
        public NewZoneEntry readFrom(IgniteDataInput input) throws IOException {
            CatalogZoneDescriptor descriptor = CatalogZoneDescriptor.SERIALIZER.readFrom(input);

            return new NewZoneEntry(descriptor);
        }

        @Override
        public void writeTo(NewZoneEntry object, IgniteDataOutput output) throws IOException {
            CatalogZoneDescriptor.SERIALIZER.writeTo(object.descriptor(), output);
        }
    }
}
