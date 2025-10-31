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

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor.Type;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.CatalogSerializationChecker.SerializerClass;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/**
 * Tests for catalog storage objects. Protocol version 2 reads protocol 2.
 */
public class CatalogSerializationCompatibilityV2ReadsV2Test extends CatalogSerializationCompatibilityTest {

    private static final Set<SerializerClass> collected = new HashSet<>();

    @Override
    protected int protocolVersion() {
        return 2;
    }

    @Override
    protected int defaultEntryVersion() {
        return 2;
    }

    @Override
    protected String dirName() {
        return "serialization_v2";
    }

    @Override
    protected boolean expectExactVersion() {
        return true;
    }

    @AfterAll
    public static void allSerializersHaveTests() {
        // 1. Collect serializers (entry class + version)
        Set<SerializerClass> serializers = CatalogSerializationChecker.findEntrySerializers()
                .stream()
                .filter(sc -> {
                    // Exclude serializers for protocol version 1
                    return !SerializationV1Classes.includes(sc);
                })
                .collect(Collectors.toSet());

        // 2. Compare entry class + version with existing serializers
        compareSerializers(serializers, collected);
    }

    @Override
    protected void recordClass(SerializerClass clazz) {
        collected.add(clazz);
    }

    @Test
    public void snapshotEntry() {
        List<CatalogZoneDescriptor> zones = TestZoneDescriptors.zones(protocolVersion(), state, defaultEntryVersion());

        EnumMap<Type, Integer> objectVersions = new EnumMap<>(CatalogObjectDescriptor.Type.class);
        objectVersions.put(Type.TABLE, defaultEntryVersion());
        objectVersions.put(Type.INDEX, defaultEntryVersion());
        objectVersions.put(Type.SYSTEM_VIEW, defaultEntryVersion());

        Catalog catalog1 = new Catalog(
                2367,
                5675344L,
                100,
                zones,
                TestSchemaDescriptors.schemas(state, 2, objectVersions),
                zones.get(0).id()
        );

        SnapshotEntry snapshotEntry = new SnapshotEntry(catalog1);

        checker.compareSnapshotEntry(snapshotEntry, "SnapshotEntry", defaultEntryVersion());
    }

    @Test
    public void snapshotEntryNoDefaultZone() {
        List<CatalogZoneDescriptor> zones = TestZoneDescriptors.zones(protocolVersion(), state, defaultEntryVersion());

        EnumMap<Type, Integer> objectVersions = new EnumMap<>(CatalogObjectDescriptor.Type.class);
        objectVersions.put(Type.TABLE, defaultEntryVersion());
        objectVersions.put(Type.INDEX, defaultEntryVersion());
        objectVersions.put(Type.SYSTEM_VIEW, defaultEntryVersion());

        Catalog catalog1 = new Catalog(
                789879,
                23432L,
                2343,
                zones,
                TestSchemaDescriptors.schemas(state, 2, objectVersions),
                null
        );

        SnapshotEntry snapshotEntry = new SnapshotEntry(catalog1);

        checker.compareSnapshotEntry(snapshotEntry, "SnapshotEntryNoDefaultZone", defaultEntryVersion());
    }

    @Test
    public void objectIdUpdate() {
        List<UpdateEntry> entries = List.of(new ObjectIdGenUpdateEntry(23431), new ObjectIdGenUpdateEntry(1204));

        checker.compareEntries(entries, "ObjectIdGenUpdateEntry", defaultEntryVersion());
    }

    // Zones

    @Test
    public void newZone() {
        List<CatalogZoneDescriptor> zones = TestZoneDescriptors.zones(protocolVersion(), state, defaultEntryVersion());
        List<UpdateEntry> entries = zones.stream().map(NewZoneEntry::new).collect(Collectors.toList());

        checker.compareEntries(entries, "NewZoneEntry", defaultEntryVersion());
    }

    @Test
    public void alterZone() {
        List<CatalogZoneDescriptor> zones = TestZoneDescriptors.zones(protocolVersion(), state, defaultEntryVersion());

        List<UpdateEntry> entries = List.of(
                new AlterZoneEntry(zones.get(1)),
                new AlterZoneEntry(zones.get(2))
        );

        checker.compareEntries(entries, "AlterZoneEntry", defaultEntryVersion());
    }

    @Test
    public void setDefaultZone() {
        List<UpdateEntry> entries = List.of(
                new SetDefaultZoneEntry(state.id()),
                new SetDefaultZoneEntry(state.id())
        );

        checker.compareEntries(entries, "SetDefaultZoneEntry", defaultEntryVersion());
    }

    @Test
    public void dropZone() {
        List<UpdateEntry> entries = List.of(
                new DropZoneEntry(state.id()),
                new DropZoneEntry(state.id())
        );

        checker.compareEntries(entries, "DropZoneEntry", defaultEntryVersion());
    }

    // Schemas

    @Test
    public void newSchema() {
        EnumMap<Type, Integer> objectVersions = new EnumMap<>(CatalogObjectDescriptor.Type.class);
        objectVersions.put(Type.TABLE, defaultEntryVersion());
        objectVersions.put(Type.INDEX, defaultEntryVersion());
        objectVersions.put(Type.SYSTEM_VIEW, defaultEntryVersion());

        List<UpdateEntry> entries = TestSchemaDescriptors.schemas(state, 2, objectVersions)
                .stream()
                .map(NewSchemaEntry::new)
                .collect(Collectors.toList());

        checker.compareEntries(entries, "NewSchemaEntry", defaultEntryVersion());
    }

    @Test
    public void dropSchema() {
        List<UpdateEntry> entries = List.of(
                new DropSchemaEntry(state.id()),
                new DropSchemaEntry(state.id())
        );

        checker.compareEntries(entries, "DropSchemaEntry", defaultEntryVersion());
    }

    // Tables

    @Test
    public void newTable() {
        int version = 2;

        List<UpdateEntry> entries = TestTableDescriptors.tables(state, version)
                .stream()
                .map(NewTableEntry::new)
                .collect(Collectors.toList());

        checker.compareEntries(entries, "NewTableEntry", version);
    }

    @Test
    public void renameTable() {
        List<UpdateEntry> entries = List.of(
                new RenameTableEntry(state.id(), "NEW_NAME1"),
                new RenameTableEntry(state.id(), "NEW_NAME2")
        );

        checker.compareEntries(entries, "RenameTableEntry", defaultEntryVersion());
    }

    @Test
    public void dropTable() {
        List<UpdateEntry> entries = List.of(
                new DropTableEntry(state.id()),
                new DropTableEntry(state.id())
        );

        checker.compareEntries(entries, "DropTableEntry", defaultEntryVersion());
    }

    // Indexes

    @Test
    public void newIndex() {
        int version = 2;

        List<UpdateEntry> entries1 = TestIndexDescriptors.sortedIndices(state, version)
                .stream()
                .map(NewIndexEntry::new)
                .collect(Collectors.toList());

        List<UpdateEntry> entries2 = TestIndexDescriptors.hashIndices(state, version)
                .stream()
                .map(NewIndexEntry::new)
                .collect(Collectors.toList());

        List<UpdateEntry> entries = new ArrayList<>(entries1);
        entries.addAll(entries2);

        Collections.shuffle(entries, state.random());

        checker.compareEntries(entries, "NewIndexEntry", version);
    }

    @Test
    public void newIndexV3() {
        int version = 3;

        List<UpdateEntry> entries1 = TestIndexDescriptors.sortedIndices(state, version)
                .stream()
                .map(NewIndexEntry::new)
                .collect(Collectors.toList());

        List<UpdateEntry> entries2 = TestIndexDescriptors.hashIndices(state, version)
                .stream()
                .map(NewIndexEntry::new)
                .collect(Collectors.toList());

        List<UpdateEntry> entries = new ArrayList<>(entries1);
        entries.addAll(entries2);

        Collections.shuffle(entries, state.random());

        checker.addExpectedVersion(MarshallableEntryType.DESCRIPTOR_HASH_INDEX.id(), version);
        checker.addExpectedVersion(MarshallableEntryType.DESCRIPTOR_SORTED_INDEX.id(), version);
        checker.compareEntries(entries, "NewIndexEntry", version);
    }

    @Test
    public void renameIndex() {
        List<UpdateEntry> entries = List.of(new RenameIndexEntry(state.id(), "NEW_NAME"));

        checker.compareEntries(entries, "RenameIndexEntry", defaultEntryVersion());
    }

    @Test
    public void removeIndex() {
        List<UpdateEntry> entries = List.of(
                new RemoveIndexEntry(state.id()),
                new RemoveIndexEntry(state.id())
        );

        checker.compareEntries(entries, "RemoveIndexEntry", defaultEntryVersion());
    }

    @Test
    public void makeIndexAvailable() {
        List<UpdateEntry> entries = List.of(
                new MakeIndexAvailableEntry(state.id()),
                new MakeIndexAvailableEntry(state.id())
        );

        checker.compareEntries(entries, "MakeIndexAvailableEntry", defaultEntryVersion());
    }

    @Test
    public void startBuildingIndex() {
        List<UpdateEntry> entries = List.of(
                new StartBuildingIndexEntry(state.id()),
                new StartBuildingIndexEntry(state.id())
        );

        checker.compareEntries(entries, "StartBuildingIndexEntry", defaultEntryVersion());
    }

    @Test
    public void dropIndex() {
        List<UpdateEntry> entries = List.of(
                new DropIndexEntry(state.id()),
                new DropIndexEntry(state.id())
        );

        checker.compareEntries(entries, "DropIndexEntry", defaultEntryVersion());
    }

    // Columns

    @Test
    public void newColumns() {
        List<CatalogTableColumnDescriptor> columns1 = TestTableColumnDescriptors.columns(state);
        List<CatalogTableColumnDescriptor> columns2 = TestTableColumnDescriptors.columns(state);

        Collections.shuffle(columns1, state.random());
        Collections.shuffle(columns2, state.random());

        List<UpdateEntry> entries = List.of(
                new NewColumnsEntry(state.id(), columns1),
                new NewColumnsEntry(state.id(), columns2)
        );

        checker.compareEntries(entries, "NewColumnsEntry", defaultEntryVersion());
    }

    @Test
    public void alterColumn() {
        List<CatalogTableColumnDescriptor> columns = TestTableColumnDescriptors.columns(state);
        Collections.shuffle(columns, state.random());

        List<UpdateEntry> entries = List.of(
                new AlterColumnEntry(state.id(), columns.get(0)),
                new AlterColumnEntry(state.id(), columns.get(1))
        );
        checker.compareEntries(entries, "AlterColumnsEntry", defaultEntryVersion());
    }

    @Test
    public void dropColumns() {
        List<CatalogTableColumnDescriptor> columns1 = TestTableColumnDescriptors.columns(state);
        List<CatalogTableColumnDescriptor> columns2 = TestTableColumnDescriptors.columns(state);

        Collections.shuffle(columns1, state.random());
        Collections.shuffle(columns2, state.random());

        List<UpdateEntry> entries = List.of(
                new DropColumnsEntry(state.id(), Set.of("C1", "C2")),
                new DropColumnsEntry(state.id(), Set.of("C3"))
        );
        checker.compareEntries(entries, "DropColumnsEntry", defaultEntryVersion());
    }

    // System views

    @Test
    public void newSystemView() {
        int version = 2;

        List<UpdateEntry> entries = TestSystemViewDescriptors.systemViews(state, version)
                .stream()
                .map(NewSystemViewEntry::new)
                .collect(Collectors.toList());

        checker.compareEntries(entries, "NewSystemViewEntry", version);
    }

    @Test
    public void alterTableProperties() {
        List<UpdateEntry> entries = List.of(
                new AlterTablePropertiesEntry(state.id(), null, null),
                new AlterTablePropertiesEntry(state.id(), 1.0d, null),
                new AlterTablePropertiesEntry(state.id(), null, 10L),
                new AlterTablePropertiesEntry(state.id(), 2.0d, 10L)
        );

        checker.addExpectedVersion(MarshallableEntryType.ALTER_TABLE_PROPERTIES.id(), 1);
        checker.compareEntries(entries, "AlterTableProperties", 1);
    }

    @Test
    public void newTableV3() {
        int tableSerializerVersion = 2;
        int tableVersionsSerializerVersion = 3;
        int tableColumnSerializerVersion = 3;
        int snapshotFileSuffix = 3;

        List<UpdateEntry> entries = TestTableDescriptors.tables(state, tableSerializerVersion)
                .stream()
                .map(NewTableEntry::new)
                .collect(Collectors.toList());

        checker.addExpectedVersion(MarshallableEntryType.DESCRIPTOR_TABLE_SCHEMA_VERSIONS.id(), tableVersionsSerializerVersion);
        checker.addExpectedVersion(MarshallableEntryType.DESCRIPTOR_TABLE_COLUMN.id(), tableColumnSerializerVersion);
        checker.compareEntries(entries, "NewTableEntry", snapshotFileSuffix);
    }
}
