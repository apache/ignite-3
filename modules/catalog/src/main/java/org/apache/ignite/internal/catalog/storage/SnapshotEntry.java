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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.jetbrains.annotations.Nullable;

/**
 * A catalog snapshot entry.
 */
public class SnapshotEntry implements UpdateLogEvent {
    public static final CatalogObjectSerializer<SnapshotEntry> SERIALIZER = new SnapshotEntrySerializer();

    private final int version;
    private final long activationTime;
    private final int objectIdGenState;
    private final CatalogZoneDescriptor[] zones;
    private final CatalogSchemaDescriptor[] schemas;
    private final @Nullable Integer defaultZoneId;

    /**
     * Constructs the object.
     *
     * @param catalog Catalog instance.
     */
    public SnapshotEntry(Catalog catalog) {
        this(catalog.version(), catalog.time(), catalog.objectIdGenState(), catalog.zones().toArray(CatalogZoneDescriptor[]::new),
                catalog.schemas().toArray(CatalogSchemaDescriptor[]::new), defaultZoneIdOpt(catalog));
    }

    /**
     * Constructs the object.
     */
    private SnapshotEntry(
            int version,
            long activationTime,
            int objectIdGenState,
            CatalogZoneDescriptor[] zones,
            CatalogSchemaDescriptor[] schemas,
            @Nullable Integer defaultZoneId
    ) {
        this.version = version;
        this.activationTime = activationTime;
        this.objectIdGenState = objectIdGenState;
        this.zones = zones;
        this.schemas = schemas;
        this.defaultZoneId = defaultZoneId;
    }

    /**
     * Returns catalog snapshot version.
     */
    public int version() {
        return version;
    }

    /**
     * Returns catalog snapshot.
     */
    public Catalog snapshot() {
        return new Catalog(
                version,
                activationTime,
                objectIdGenState,
                List.of(zones),
                List.of(schemas),
                defaultZoneId
        );
    }

    @Override
    public int typeId() {
        return MarshallableEntryType.SNAPSHOT.id();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SnapshotEntry that = (SnapshotEntry) o;
        return version == that.version && activationTime == that.activationTime && objectIdGenState == that.objectIdGenState
                && Arrays.equals(zones, that.zones) && Arrays.equals(schemas, that.schemas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }

    /** Serializer for {@link SnapshotEntry}. */
    private static class SnapshotEntrySerializer implements CatalogObjectSerializer<SnapshotEntry> {
        @Override
        public SnapshotEntry readFrom(IgniteDataInput input) throws IOException {
            int catalogVersion = input.readInt();
            long activationTime = input.readLong();
            int objectIdGenState = input.readInt();

            CatalogZoneDescriptor[] zones =
                    CatalogSerializationUtils.readArray(CatalogZoneDescriptor.SERIALIZER, input, CatalogZoneDescriptor.class);

            CatalogSchemaDescriptor[] schemas =
                    CatalogSerializationUtils.readArray(CatalogSchemaDescriptor.SERIALIZER, input, CatalogSchemaDescriptor.class);

            Integer defaultZoneId = null;
            if (input.readBoolean()) {
                defaultZoneId = input.readInt();
            }

            return new SnapshotEntry(catalogVersion, activationTime, objectIdGenState, zones, schemas, defaultZoneId);
        }

        @Override
        public void writeTo(SnapshotEntry entry, IgniteDataOutput output) throws IOException {
            output.writeInt(entry.version);
            output.writeLong(entry.activationTime);
            output.writeInt(entry.objectIdGenState);

            CatalogSerializationUtils.writeArray(entry.zones, CatalogZoneDescriptor.SERIALIZER, output);
            CatalogSerializationUtils.writeArray(entry.schemas, CatalogSchemaDescriptor.SERIALIZER, output);

            output.writeBoolean(entry.defaultZoneId != null);
            if (entry.defaultZoneId != null) {
                output.writeInt(entry.defaultZoneId);
            }
        }
    }
}
