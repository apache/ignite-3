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

import java.util.Set;
import org.apache.ignite.internal.catalog.storage.CatalogSerializationChecker.SerializerClass;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;

/**
 * A set of objects that have serialization format version 1 serializers.
 */
final class SerializationV1Classes {

    // V1 in nested object serializers are called manually. 
    private static final Set<SerializerClass> DESCRIPTORS = Set.of(
            new SerializerClass(MarshallableEntryType.DESCRIPTOR_HASH_INDEX.id(), 1),
            new SerializerClass(MarshallableEntryType.DESCRIPTOR_SORTED_INDEX.id(), 1),
            new SerializerClass(MarshallableEntryType.DESCRIPTOR_STORAGE_PROFILE.id(), 1),
            new SerializerClass(MarshallableEntryType.DESCRIPTOR_STORAGE_PROFILES.id(), 1),
            new SerializerClass(MarshallableEntryType.DESCRIPTOR_SYSTEM_VIEW.id(), 1),
            new SerializerClass(MarshallableEntryType.DESCRIPTOR_SCHEMA.id(), 1),
            new SerializerClass(MarshallableEntryType.DESCRIPTOR_TABLE.id(), 1),
            new SerializerClass(MarshallableEntryType.DESCRIPTOR_TABLE_COLUMN.id(), 1),
            new SerializerClass(MarshallableEntryType.DESCRIPTOR_TABLE_VERSION.id(), 1),
            new SerializerClass(MarshallableEntryType.DESCRIPTOR_TABLE_SCHEMA_VERSIONS.id(), 1),
            new SerializerClass(MarshallableEntryType.DESCRIPTOR_ZONE.id(), 1)
    );

    private static final Set<SerializerClass> ENTRIES = Set.of(
            new SerializerClass(MarshallableEntryType.ALTER_COLUMN.id(), 1),
            new SerializerClass(MarshallableEntryType.ALTER_ZONE.id(), 1),
            new SerializerClass(MarshallableEntryType.DROP_COLUMN.id(), 1),
            new SerializerClass(MarshallableEntryType.DROP_INDEX.id(), 1),
            new SerializerClass(MarshallableEntryType.DROP_SCHEMA.id(), 1),
            new SerializerClass(MarshallableEntryType.DROP_TABLE.id(), 1),
            new SerializerClass(MarshallableEntryType.DROP_ZONE.id(), 1),
            new SerializerClass(MarshallableEntryType.MAKE_INDEX_AVAILABLE.id(), 1),
            new SerializerClass(MarshallableEntryType.NEW_COLUMN.id(), 1),
            new SerializerClass(MarshallableEntryType.NEW_INDEX.id(), 1),
            new SerializerClass(MarshallableEntryType.NEW_SCHEMA.id(), 1),
            new SerializerClass(MarshallableEntryType.NEW_SYS_VIEW.id(), 1),
            new SerializerClass(MarshallableEntryType.NEW_TABLE.id(), 1),
            new SerializerClass(MarshallableEntryType.NEW_ZONE.id(), 1),
            new SerializerClass(MarshallableEntryType.ID_GENERATOR.id(), 1),
            new SerializerClass(MarshallableEntryType.REMOVE_INDEX.id(), 1),
            new SerializerClass(MarshallableEntryType.RENAME_INDEX.id(), 1),
            new SerializerClass(MarshallableEntryType.RENAME_TABLE.id(), 1),
            new SerializerClass(MarshallableEntryType.SET_DEFAULT_ZONE.id(), 1),
            new SerializerClass(MarshallableEntryType.SNAPSHOT.id(), 1),
            new SerializerClass(MarshallableEntryType.START_BUILDING_INDEX.id(), 1),
            new SerializerClass(MarshallableEntryType.VERSIONED_UPDATE.id(), 1)
    );

    private SerializationV1Classes() {

    }

    static boolean includesDescriptor(SerializerClass serializerClass) {
        return DESCRIPTORS.contains(serializerClass);
    }

    static boolean includes(SerializerClass serializerClass) {
        return ENTRIES.contains(serializerClass) || DESCRIPTORS.contains(serializerClass);
    }
}
