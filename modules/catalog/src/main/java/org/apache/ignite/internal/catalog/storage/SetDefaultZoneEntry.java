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
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.DropZoneEventParameters;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Describes changing of a default zone.
 */
public class SetDefaultZoneEntry implements UpdateEntry, Fireable {
    public static final CatalogObjectSerializer<SetDefaultZoneEntry> SERIALIZER = new SetDefaultZoneEntrySerializer();

    private final int zoneId;

    /**
     * Constructs the object.
     *
     * @param zoneId An id of a zone to set default.
     */
    public SetDefaultZoneEntry(int zoneId) {
        this.zoneId = zoneId;
    }

    /** Returns an id of a zone to set default. */
    public int zoneId() {
        return zoneId;
    }

    @Override
    public int typeId() {
        return MarshallableEntryType.SET_DEFAULT_ZONE.id();
    }

    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.SET_DEFAULT_ZONE;
    }

    @Override
    public CatalogEventParameters createEventParameters(long causalityToken, int catalogVersion) {
        return new DropZoneEventParameters(causalityToken, catalogVersion, zoneId);
    }

    @Override
    public Catalog applyUpdate(Catalog catalog, long causalityToken) {
        return new Catalog(
                catalog.version(),
                catalog.time(),
                catalog.objectIdGenState(),
                catalog.zones(),
                catalog.schemas(),
                zoneId);
    }

    @Override
    public String toString() {
        return S.toString(this);
    }

    /**
     * Serializer for {@link SetDefaultZoneEntry}.
     */
    private static class SetDefaultZoneEntrySerializer implements CatalogObjectSerializer<SetDefaultZoneEntry> {
        @Override
        public SetDefaultZoneEntry readFrom(IgniteDataInput input) throws IOException {
            int zoneId = input.readInt();

            return new SetDefaultZoneEntry(zoneId);
        }

        @Override
        public void writeTo(SetDefaultZoneEntry entry, IgniteDataOutput output) throws IOException {
            output.writeInt(entry.zoneId());
        }
    }
}
