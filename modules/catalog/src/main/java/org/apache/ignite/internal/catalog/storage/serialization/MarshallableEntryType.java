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

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptorSerializers;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptorSerializers;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptorSerializers;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfileDescriptorSerializers;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfilesDescriptorSerializers;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptorSerializers;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptorSerializers;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptorSerializers;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableSchemaVersionsSerializers;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableVersionSerializers;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptorSerializers;
import org.apache.ignite.internal.catalog.storage.AlterColumnEntrySerializers;
import org.apache.ignite.internal.catalog.storage.AlterTablePropertiesEntrySerializers;
import org.apache.ignite.internal.catalog.storage.AlterZoneEntrySerializers;
import org.apache.ignite.internal.catalog.storage.DropColumnsEntrySerializers;
import org.apache.ignite.internal.catalog.storage.DropIndexEntrySerializers;
import org.apache.ignite.internal.catalog.storage.DropSchemaSerializers;
import org.apache.ignite.internal.catalog.storage.DropTableEntrySerializers;
import org.apache.ignite.internal.catalog.storage.DropZoneEntrySerializers;
import org.apache.ignite.internal.catalog.storage.MakeIndexAvailableEntrySerializers;
import org.apache.ignite.internal.catalog.storage.NewColumnsEntrySerializers;
import org.apache.ignite.internal.catalog.storage.NewIndexEntrySerializers;
import org.apache.ignite.internal.catalog.storage.NewSchemaEntrySerializers;
import org.apache.ignite.internal.catalog.storage.NewSystemViewEntrySerializers;
import org.apache.ignite.internal.catalog.storage.NewTableEntrySerializers;
import org.apache.ignite.internal.catalog.storage.NewZoneEntrySerializers;
import org.apache.ignite.internal.catalog.storage.ObjectIdGenUpdateEntrySerializers;
import org.apache.ignite.internal.catalog.storage.RemoveIndexEntrySerializers;
import org.apache.ignite.internal.catalog.storage.RenameIndexEntrySerializers;
import org.apache.ignite.internal.catalog.storage.RenameTableEntrySerializers;
import org.apache.ignite.internal.catalog.storage.SetDefaultZoneEntrySerializers;
import org.apache.ignite.internal.catalog.storage.SnapshotEntrySerializers;
import org.apache.ignite.internal.catalog.storage.StartBuildingIndexEntrySerializers;
import org.apache.ignite.internal.catalog.storage.VersionedUpdateSerializers;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Enumeration of all serializable catalog objects.
 */
public enum MarshallableEntryType implements CatalogSerializerTypeDefinition {
    ALTER_COLUMN(0, AlterColumnEntrySerializers.class),
    ALTER_ZONE(1, AlterZoneEntrySerializers.class),
    NEW_ZONE(2, NewZoneEntrySerializers.class),
    DROP_COLUMN(3, DropColumnsEntrySerializers.class),
    DROP_INDEX(4, DropIndexEntrySerializers.class),
    DROP_TABLE(5, DropTableEntrySerializers.class),
    DROP_ZONE(6, DropZoneEntrySerializers.class),
    MAKE_INDEX_AVAILABLE(7, MakeIndexAvailableEntrySerializers.class),
    REMOVE_INDEX(8, RemoveIndexEntrySerializers.class),
    START_BUILDING_INDEX(9, StartBuildingIndexEntrySerializers.class),
    NEW_COLUMN(10, NewColumnsEntrySerializers.class),
    NEW_INDEX(11, NewIndexEntrySerializers.class),
    NEW_SYS_VIEW(12, NewSystemViewEntrySerializers.class),
    NEW_TABLE(13, NewTableEntrySerializers.class),
    RENAME_TABLE(14, RenameTableEntrySerializers.class),
    ID_GENERATOR(15, ObjectIdGenUpdateEntrySerializers.class),
    SNAPSHOT(16, SnapshotEntrySerializers.class),
    VERSIONED_UPDATE(17, VersionedUpdateSerializers.class),
    RENAME_INDEX(18, RenameIndexEntrySerializers.class),
    SET_DEFAULT_ZONE(19, SetDefaultZoneEntrySerializers.class),
    NEW_SCHEMA(20, NewSchemaEntrySerializers.class),
    DROP_SCHEMA(21, DropSchemaSerializers.class),
    DESCRIPTOR_HASH_INDEX(22, CatalogHashIndexDescriptorSerializers.class),
    DESCRIPTOR_SORTED_INDEX(23, CatalogSortedIndexDescriptorSerializers.class),
    DESCRIPTOR_SCHEMA(24, CatalogSchemaDescriptorSerializers.class),
    DESCRIPTOR_STORAGE_PROFILE(25, CatalogStorageProfileDescriptorSerializers.class),
    DESCRIPTOR_STORAGE_PROFILES(26, CatalogStorageProfilesDescriptorSerializers.class),
    DESCRIPTOR_SYSTEM_VIEW(27, CatalogSystemViewDescriptorSerializers.class),
    DESCRIPTOR_TABLE(28, CatalogTableDescriptorSerializers.class),
    DESCRIPTOR_TABLE_COLUMN(29, CatalogTableColumnDescriptorSerializers.class),
    DESCRIPTOR_TABLE_VERSION(30, CatalogTableVersionSerializers.class),
    DESCRIPTOR_TABLE_SCHEMA_VERSIONS(31, CatalogTableSchemaVersionsSerializers.class),
    DESCRIPTOR_ZONE(32, CatalogZoneDescriptorSerializers.class),
    ALTER_TABLE_PROPERTIES(33, AlterTablePropertiesEntrySerializers.class);

    /** Type ID. */
    private final int id;

    /** Serializer container class. */
    private final Class<?> serializerContainer;

    private static final MarshallableEntryType[] VALS = new MarshallableEntryType[values().length];

    static {
        Set<Class<?>> containerClasses = new HashSet<>(IgniteUtils.capacity(VALS.length));

        for (MarshallableEntryType entryType : values()) {
            assert VALS[entryType.id] == null : "Found duplicate id " + entryType.id;

            if (!containerClasses.add(entryType.serializerContainer)) {
                throw new IllegalStateException("Found duplicate serializer container "
                        + "[class=" + entryType.serializerContainer.getCanonicalName() + "].");
            }

            VALS[entryType.id()] = entryType;
        }
    }

    MarshallableEntryType(int id, Class<?> serializerContainer) {
        this.id = id;
        this.serializerContainer = serializerContainer;
    }

    @Override
    public int id() {
        return id;
    }

    @Override
    public Class<?> container() {
        return serializerContainer;
    }
}
