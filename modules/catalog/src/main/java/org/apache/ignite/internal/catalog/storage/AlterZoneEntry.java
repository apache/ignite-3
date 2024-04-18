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

import static java.util.stream.Collectors.toList;

import java.io.IOException;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.AlterZoneEventParameters;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Describes altering zone.
 */
public class AlterZoneEntry implements UpdateEntry, Fireable {
    public static final CatalogObjectSerializer<AlterZoneEntry> SERIALIZER = new AlterZoneEntrySerializer();

    private final CatalogZoneDescriptor descriptor;

    /**
     * Constructs the object.
     *
     * @param descriptor A descriptor of a zone to alter.
     */
    public AlterZoneEntry(CatalogZoneDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    /** Returns descriptor of a zone to alter. */
    public CatalogZoneDescriptor descriptor() {
        return descriptor;
    }

    @Override
    public int typeId() {
        return MarshallableEntryType.ALTER_ZONE.id();
    }

    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.ZONE_ALTER;
    }

    @Override
    public CatalogEventParameters createEventParameters(long causalityToken, int catalogVersion) {
        return new AlterZoneEventParameters(causalityToken, catalogVersion, descriptor);
    }

    @Override
    public Catalog applyUpdate(Catalog catalog, long causalityToken) {
        descriptor.updateToken(causalityToken);

        return new Catalog(
                catalog.version(),
                catalog.time(),
                catalog.objectIdGenState(),
                catalog.zones().stream()
                        .map(z -> z.id() == descriptor.id() ? descriptor : z)
                        .collect(toList()),
                catalog.schemas(),
                catalog.defaultZone().id()
        );
    }

    @Override
    public String toString() {
        return S.toString(this);
    }

    /**
     * Serializer for {@link AlterZoneEntry}.
     */
    private static class AlterZoneEntrySerializer implements CatalogObjectSerializer<AlterZoneEntry> {
        @Override
        public AlterZoneEntry readFrom(IgniteDataInput input) throws IOException {
            CatalogZoneDescriptor descriptor = CatalogZoneDescriptor.SERIALIZER.readFrom(input);

            return new AlterZoneEntry(descriptor);
        }

        @Override
        public void writeTo(AlterZoneEntry object, IgniteDataOutput output) throws IOException {
            CatalogZoneDescriptor.SERIALIZER.writeTo(object.descriptor(), output);
        }
    }
}
