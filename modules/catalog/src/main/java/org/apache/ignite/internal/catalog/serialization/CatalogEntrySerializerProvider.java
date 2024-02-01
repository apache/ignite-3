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
        @Override
        public CatalogObjectSerializer<MarshallableEntry> get(int typeId) {
            CatalogObjectSerializer<? extends MarshallableEntry> serializer;
            MarshallableEntryType type = MarshallableEntryType.forId(typeId);

            switch (type) {
                case ALTER_COLUMN:
                    serializer = AlterColumnEntry.SERIALIZER;
                    break;

                case ALTER_ZONE:
                    serializer = AlterZoneEntry.SERIALIZER;
                    break;

                case NEW_ZONE:
                    serializer = NewZoneEntry.SERIALIZER;
                    break;

                case DROP_COLUMN:
                    serializer = DropColumnsEntry.SERIALIZER;
                    break;

                case DROP_INDEX:
                    serializer = DropIndexEntry.SERIALIZER;
                    break;

                case DROP_TABLE:
                    serializer = DropTableEntry.SERIALIZER;
                    break;

                case DROP_ZONE:
                    serializer = DropZoneEntry.SERIALIZER;
                    break;

                case MAKE_INDEX_AVAILABLE:
                    serializer = MakeIndexAvailableEntry.SERIALIZER;
                    break;

                case REMOVE_INDEX:
                    serializer = RemoveIndexEntry.SERIALIZER;
                    break;

                case START_BUILDING_INDEX:
                    serializer = StartBuildingIndexEntry.SERIALIZER;
                    break;

                case NEW_COLUMN:
                    serializer = NewColumnsEntry.SERIALIZER;
                    break;

                case NEW_INDEX:
                    serializer = NewIndexEntry.SERIALIZER;
                    break;

                case NEW_SYS_VIEW:
                    serializer = NewSystemViewEntry.SERIALIZER;
                    break;

                case NEW_TABLE:
                    serializer = NewTableEntry.SERIALIZER;
                    break;

                case RENAME_TABLE:
                    serializer = RenameTableEntry.SERIALIZER;
                    break;

                case ID_GENERATOR:
                    serializer = ObjectIdGenUpdateEntry.SERIALIZER;
                    break;

                case SNAPSHOT:
                    serializer = SnapshotEntry.SERIALIZER;
                    break;

                case VERSIONED_UPDATE:
                    serializer = VersionedUpdate.SERIALIZER;
                    break;

                default:
                    throw new IllegalArgumentException("Unsupported type: " + type);
            }

            return (CatalogObjectSerializer<MarshallableEntry>) serializer;
        }
    };
}
