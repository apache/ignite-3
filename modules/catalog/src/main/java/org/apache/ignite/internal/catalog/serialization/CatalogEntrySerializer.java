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

import java.io.IOException;
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
import org.apache.ignite.internal.catalog.storage.RenameTableEntry;
import org.apache.ignite.internal.catalog.storage.StartBuildingIndexEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntryType;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Catalog entry serializer.
 */
public interface CatalogEntrySerializer<T> {
    /**
     * Reads catalog entry object from data input.
     *
     * @param version Data format version.
     * @param input Data input.
     * @return Catalog entry.
     */
    T readFrom(int version, IgniteDataInput input) throws IOException;

    /**
     * Writes catalog entry to data input.
     *
     * @param value Catalog entry.
     * @param version Required data format version.
     * @param output Data output.
     */
    void writeTo(T value, int version, IgniteDataOutput output) throws IOException;

    /** Returns entry serializer for the specified type. */
    static <T extends UpdateEntry> CatalogEntrySerializer<T> forTypeId(int typeId) {
        UpdateEntryType type = UpdateEntryType.getById(typeId);

        switch (type) {
            case ALTER_ZONE:
                return (CatalogEntrySerializer<T>) AlterZoneEntry.SERIALIZER;

            case NEW_ZONE:
                return (CatalogEntrySerializer<T>) NewZoneEntry.SERIALIZER;

            case ALTER_COLUMN:
                return (CatalogEntrySerializer<T>) AlterColumnEntry.SERIALIZER;

            case DROP_COLUMN:
                return (CatalogEntrySerializer<T>) DropColumnsEntry.SERIALIZER;

            case DROP_INDEX:
                return (CatalogEntrySerializer<T>) DropIndexEntry.SERIALIZER;

            case DROP_TABLE:
                return (CatalogEntrySerializer<T>) DropTableEntry.SERIALIZER;

            case DROP_ZONE:
                return (CatalogEntrySerializer<T>) DropZoneEntry.SERIALIZER;

            case MAKE_INDEX_AVAILABLE:
                return (CatalogEntrySerializer<T>) MakeIndexAvailableEntry.SERIALIZER;

            case START_BUILDING_INDEX:
                return (CatalogEntrySerializer<T>) StartBuildingIndexEntry.SERIALIZER;

            case NEW_COLUMN:
                return (CatalogEntrySerializer<T>) NewColumnsEntry.SERIALIZER;

            case NEW_INDEX:
                return (CatalogEntrySerializer<T>) NewIndexEntry.SERIALIZER;

            case NEW_SYS_VIEW:
                return (CatalogEntrySerializer<T>) NewSystemViewEntry.SERIALIZER;

            case NEW_TABLE:
                return (CatalogEntrySerializer<T>) NewTableEntry.SERIALIZER;

            case RENAME_TABLE:
                return (CatalogEntrySerializer<T>) RenameTableEntry.SERIALIZER;

            case ID_GENERATOR:
                return (CatalogEntrySerializer<T>) ObjectIdGenUpdateEntry.SERIALIZER;

            default:
                throw new UnsupportedOperationException("Serialization is not supported for type: " + type);
        }
    }
}
