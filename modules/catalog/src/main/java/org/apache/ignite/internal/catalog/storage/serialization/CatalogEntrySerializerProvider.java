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

package org.apache.ignite.internal.catalog.storage.serialization;

import java.util.Objects;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.storage.AlterColumnEntry;
import org.apache.ignite.internal.catalog.storage.AlterZoneEntry;
import org.apache.ignite.internal.catalog.storage.DropColumnsEntry;
import org.apache.ignite.internal.catalog.storage.DropIndexEntry;
import org.apache.ignite.internal.catalog.storage.DropTableEntry;
import org.apache.ignite.internal.catalog.storage.DropZoneEntry;
import org.apache.ignite.internal.catalog.storage.MakeIndexAvailableEntry;
import org.apache.ignite.internal.catalog.storage.NewColumnsEntry;
import org.apache.ignite.internal.catalog.storage.NewIndexEntry;
import org.apache.ignite.internal.catalog.storage.NewSystemViewEntry;
import org.apache.ignite.internal.catalog.storage.NewTableEntry;
import org.apache.ignite.internal.catalog.storage.NewZoneEntry;
import org.apache.ignite.internal.catalog.storage.ObjectIdGenUpdateEntry;
import org.apache.ignite.internal.catalog.storage.RemoveIndexEntry;
import org.apache.ignite.internal.catalog.storage.RenameIndexEntry;
import org.apache.ignite.internal.catalog.storage.RenameTableEntry;
import org.apache.ignite.internal.catalog.storage.SetDefaultZoneEntry;
import org.apache.ignite.internal.catalog.storage.SnapshotEntry;
import org.apache.ignite.internal.catalog.storage.StartBuildingIndexEntry;
import org.apache.ignite.internal.catalog.storage.VersionedUpdate.VersionedUpdateSerializer;

/**
 * Catalog entry serializer provider.
 */
@SuppressWarnings("InterfaceMayBeAnnotatedFunctional")
public interface CatalogEntrySerializerProvider {
    /**
     * Gets a catalog entry serializer that supports serialization of the specified type.
     *
     * @param typeId Type id.
     * @return catalog update entry serializer.
     */
    CatalogObjectSerializer<MarshallableEntry> get(int typeId);

    /** Default implementation. */
    CatalogEntrySerializerProvider DEFAULT_PROVIDER = new CatalogEntrySerializerProvider() {
        @SuppressWarnings("unchecked")
        private final CatalogObjectSerializer<? extends MarshallableEntry>[] serializers =
                new CatalogObjectSerializer[MarshallableEntryType.values().length];

        {
            serializers[MarshallableEntryType.ALTER_COLUMN.id()] = AlterColumnEntry.SERIALIZER;
            serializers[MarshallableEntryType.ALTER_ZONE.id()] = AlterZoneEntry.SERIALIZER;
            serializers[MarshallableEntryType.NEW_ZONE.id()] = NewZoneEntry.SERIALIZER;
            serializers[MarshallableEntryType.DROP_COLUMN.id()] = DropColumnsEntry.SERIALIZER;
            serializers[MarshallableEntryType.DROP_INDEX.id()] = DropIndexEntry.SERIALIZER;
            serializers[MarshallableEntryType.DROP_TABLE.id()] = DropTableEntry.SERIALIZER;
            serializers[MarshallableEntryType.DROP_ZONE.id()] = DropZoneEntry.SERIALIZER;
            serializers[MarshallableEntryType.MAKE_INDEX_AVAILABLE.id()] = MakeIndexAvailableEntry.SERIALIZER;
            serializers[MarshallableEntryType.REMOVE_INDEX.id()] = RemoveIndexEntry.SERIALIZER;
            serializers[MarshallableEntryType.START_BUILDING_INDEX.id()] = StartBuildingIndexEntry.SERIALIZER;
            serializers[MarshallableEntryType.NEW_COLUMN.id()] = NewColumnsEntry.SERIALIZER;
            serializers[MarshallableEntryType.NEW_INDEX.id()] = NewIndexEntry.SERIALIZER;
            serializers[MarshallableEntryType.NEW_SYS_VIEW.id()] = NewSystemViewEntry.SERIALIZER;
            serializers[MarshallableEntryType.NEW_TABLE.id()] = NewTableEntry.SERIALIZER;
            serializers[MarshallableEntryType.RENAME_TABLE.id()] = RenameTableEntry.SERIALIZER;
            serializers[MarshallableEntryType.ID_GENERATOR.id()] = ObjectIdGenUpdateEntry.SERIALIZER;
            serializers[MarshallableEntryType.SNAPSHOT.id()] = SnapshotEntry.SERIALIZER;
            serializers[MarshallableEntryType.RENAME_INDEX.id()] = RenameIndexEntry.SERIALIZER;
            serializers[MarshallableEntryType.SET_DEFAULT_ZONE.id()] = SetDefaultZoneEntry.SERIALIZER;
            //noinspection ThisEscapedInObjectConstruction
            serializers[MarshallableEntryType.VERSIONED_UPDATE.id()] = new VersionedUpdateSerializer(this);

            assert Stream.of(serializers).noneMatch(Objects::isNull);
        }

        @Override
        public CatalogObjectSerializer<MarshallableEntry> get(int typeId) {
            if (typeId < 0 || typeId > serializers.length) {
                throw new IllegalArgumentException("Unknown type ID: " + typeId);
            }

            return (CatalogObjectSerializer<MarshallableEntry>) serializers[typeId];
        }
    };
}
