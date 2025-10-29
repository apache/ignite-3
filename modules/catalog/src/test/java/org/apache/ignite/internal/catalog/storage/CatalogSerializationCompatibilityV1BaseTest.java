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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for catalog storage objects. Protocol version 1.
 *
 * @deprecated Catalog serialization format version 1 was deprecated.
 */
@Deprecated
public abstract class CatalogSerializationCompatibilityV1BaseTest extends CatalogSerializationCompatibilityTest {

    private static final Set<SerializerClass> collected = new HashSet<>();

    @Override
    protected String dirName() {
        return "serialization_v1";
    }

    @AfterAll
    public static void allSerializersHaveTests() {
        // 1. Collect serializers (entry class + version)
        Set<SerializerClass> serializers = CatalogSerializationChecker.findEntrySerializers()
                .stream()
                // Exclude descriptors - their serializers are called manually.
                .filter(c -> !SerializationV1Classes.includesDescriptor(c))
                .filter(SerializationV1Classes::includes)
                .collect(Collectors.toSet());

        // 2. Compare entry class + version with existing serializers
        compareSerializers(serializers, collected);
    }

    @Override
    protected void recordClass(SerializerClass clazz) {
        if (collected.add(clazz)) {
            log.info("Record class {}", clazz);
        }
    }

    @BeforeEach
    public void setup() {
        checker.addClassesManually(true);
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
                TestSchemaDescriptors.schemas(state, 1, objectVersions),
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
                TestSchemaDescriptors.schemas(state, 1, objectVersions),
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

        List<UpdateEntry> entries = TestSchemaDescriptors.schemas(state, 1, objectVersions)
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
        int version = 1;

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
        int version = 1;

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
        int version = 1;

        List<UpdateEntry> entries = TestSystemViewDescriptors.systemViews(state, version)
                .stream()
                .map(NewSystemViewEntry::new)
                .collect(Collectors.toList());

        checker.compareEntries(entries, "NewSystemViewEntry", version);
    }
}
