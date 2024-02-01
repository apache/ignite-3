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

package org.apache.ignite.internal.catalog.serialization;

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
import org.apache.ignite.internal.catalog.storage.RenameTableEntry;
import org.apache.ignite.internal.catalog.storage.SnapshotEntry;
import org.apache.ignite.internal.catalog.storage.StartBuildingIndexEntry;
import org.apache.ignite.internal.catalog.storage.VersionedUpdate;

/**
 * Update entry serialization type.
 */
public enum MarshallableEntryType {
    ALTER_COLUMN(0, AlterColumnEntry.SERIALIZER),
    ALTER_ZONE(1, AlterZoneEntry.SERIALIZER),
    NEW_ZONE(2, NewZoneEntry.SERIALIZER),
    DROP_COLUMN(3, DropColumnsEntry.SERIALIZER),
    DROP_INDEX(4, DropIndexEntry.SERIALIZER),
    DROP_TABLE(5, DropTableEntry.SERIALIZER),
    DROP_ZONE(6, DropZoneEntry.SERIALIZER),
    MAKE_INDEX_AVAILABLE(7, MakeIndexAvailableEntry.SERIALIZER),
    REMOVE_INDEX(8, RemoveIndexEntry.SERIALIZER),
    START_BUILDING_INDEX(9, StartBuildingIndexEntry.SERIALIZER),
    NEW_COLUMN(10, NewColumnsEntry.SERIALIZER),
    NEW_INDEX(11, NewIndexEntry.SERIALIZER),
    NEW_SYS_VIEW(12, NewSystemViewEntry.SERIALIZER),
    NEW_TABLE(13, NewTableEntry.SERIALIZER),
    RENAME_TABLE(14, RenameTableEntry.SERIALIZER),
    ID_GENERATOR(15, ObjectIdGenUpdateEntry.SERIALIZER),
    SNAPSHOT(16, SnapshotEntry.SERIALIZER),
    VERSIONED_UPDATE(17, VersionedUpdate.SERIALIZER);

    /** Type ID. */
    private final int id;

    /** Serializer for this entry type. */
    private final CatalogObjectSerializer<? extends MarshallableEntry> serializer;

    private static final MarshallableEntryType[] VALS = new MarshallableEntryType[values().length];

    static {
        for (MarshallableEntryType entryType : values()) {
            assert VALS[entryType.id] == null : "Found duplicate id " + entryType.id;

            VALS[entryType.id()] = entryType;
        }
    }

    MarshallableEntryType(int id, CatalogObjectSerializer<? extends MarshallableEntry> serializer) {
        this.id = id;
        this.serializer = serializer;
    }

    /** Returns type ID. */
    public int id() {
        return id;
    }

    /** Returns serializer for this entry type. */
    public <T extends MarshallableEntry> CatalogObjectSerializer<T> serializer() {
        return (CatalogObjectSerializer<T>) serializer;
    }

    // TODO Remove
    public static CatalogEntrySerializerProvider provider() {
        return (id) -> (CatalogObjectSerializer<MarshallableEntry>) forId(id).serializer;
    }

    /** Returns entry type by identifier. */
    static MarshallableEntryType forId(int id) {
        if (id >= 0 && id < VALS.length) {
            return VALS[id];
        }

        throw new IllegalArgumentException("Unknown entry type identifier: " + id);
    }
}
