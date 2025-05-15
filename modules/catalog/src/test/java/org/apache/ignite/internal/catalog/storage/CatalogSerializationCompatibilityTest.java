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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor.Type;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for catalog storage objects. Protocol version 1.
 *
 * <p>How to add a test case:
 * <ul>
 *     <li>Update descriptor generator(s) to return version(s) you want to test.
 *     See {@link TestTableDescriptors}, {@link TestZoneDescriptors}, and so on.</li>
 *     <li>Copy an existing test case.</li>
 *     <li>Set versions to those you what to check.</li>
 *     <li>Set {@link #WRITE_SNAPSHOT}  to {@code true} to generate a snapshot.</li>
 *     <li>Run test case to generate a snapshot, set {@link #WRITE_SNAPSHOT} back to false.</li>
 * </ul>
 * <pre>
 *     public void newTableV42() {
 *         int version = 42;
 *
 *         // Generates table descriptors for version 42.
 *         List&lt;UpdateEntry&gt; entries = TestTableDescriptors.tables(state, version)
 *                 .stream()
 *                 .map(NewTableEntry::new)
 *                 .collect(Collectors.toList());
 *
 *         // Use serializer version 42 for all CatalogTableDescriptors in this test case.
 *         checker.addExpectedVersion(MarshallableEntryType.DESCRIPTOR_TABLE.id(), version);
 *         checker.compareEntries(entries, "NewTableEntry", version);
 *     }
 * </pre>
 */
public abstract class CatalogSerializationCompatibilityTest extends BaseIgniteAbstractTest {

    /** Whether to write entries to the test directory. */
    private static final boolean WRITE_SNAPSHOT = false;

    final TestDescriptorState state = new TestDescriptorState(42);

    CatalogSerializationChecker checker;

    @BeforeEach
    public void init() {
        checker = new CatalogSerializationChecker(
                log,
                dirName(),
                entryVersion(),
                expectExactVersion(),
                protocolVersion()
        );
        checker.writeSnapshot(WRITE_SNAPSHOT);
    }

    @AfterEach
    public void reset() {
        checker.reset();
    }

    @Test
    public void snapshotEntry() {
        List<CatalogZoneDescriptor> zones = TestZoneDescriptors.zones(protocolVersion(), state, entryVersion());

        EnumMap<Type, Integer> objectVersions = new EnumMap<>(CatalogObjectDescriptor.Type.class);
        objectVersions.put(Type.TABLE, entryVersion());
        objectVersions.put(Type.INDEX, entryVersion());
        objectVersions.put(Type.SYSTEM_VIEW, entryVersion());

        Catalog catalog1 = new Catalog(
                2367,
                5675344L,
                100,
                zones,
                TestSchemaDescriptors.schemas(state, entryVersion(), objectVersions),
                zones.get(0).id()
        );

        SnapshotEntry snapshotEntry = new SnapshotEntry(catalog1);

        checker.compareSnapshotEntry(snapshotEntry, "SnapshotEntry", entryVersion());
    }

    @Test
    public void snapshotEntryNoDefaultZone() {
        List<CatalogZoneDescriptor> zones = TestZoneDescriptors.zones(protocolVersion(), state, entryVersion());

        EnumMap<Type, Integer> objectVersions = new EnumMap<>(CatalogObjectDescriptor.Type.class);
        objectVersions.put(Type.TABLE, entryVersion());
        objectVersions.put(Type.INDEX, entryVersion());
        objectVersions.put(Type.SYSTEM_VIEW, entryVersion());

        Catalog catalog1 = new Catalog(
                789879,
                23432L,
                2343,
                zones,
                TestSchemaDescriptors.schemas(state, entryVersion(), objectVersions),
                null
        );

        SnapshotEntry snapshotEntry = new SnapshotEntry(catalog1);

        checker.compareSnapshotEntry(snapshotEntry, "SnapshotEntryNoDefaultZone", entryVersion());
    }

    @Test
    public void objectIdUpdate() {
        List<UpdateEntry> entries = List.of(new ObjectIdGenUpdateEntry(23431), new ObjectIdGenUpdateEntry(1204));

        checker.compareEntries(entries, "ObjectIdGenUpdateEntry", entryVersion());
    }

    // Zones

    @Test
    public void newZone() {
        List<CatalogZoneDescriptor> zones = TestZoneDescriptors.zones(protocolVersion(), state, entryVersion());
        List<UpdateEntry> entries = zones.stream().map(NewZoneEntry::new).collect(Collectors.toList());

        checker.compareEntries(entries, "NewZoneEntry", entryVersion());
    }

    @Test
    public void alterZone() {
        List<CatalogZoneDescriptor> zones = TestZoneDescriptors.zones(protocolVersion(), state, entryVersion());

        List<UpdateEntry> entries = List.of(
                new AlterZoneEntry(zones.get(1)),
                new AlterZoneEntry(zones.get(2))
        );

        checker.compareEntries(entries, "AlterZoneEntry", entryVersion());
    }

    @Test
    public void setDefaultZone() {
        List<UpdateEntry> entries = List.of(
                new SetDefaultZoneEntry(state.id()),
                new SetDefaultZoneEntry(state.id())
        );

        checker.compareEntries(entries, "SetDefaultZoneEntry", entryVersion());
    }

    @Test
    public void dropZone() {
        List<UpdateEntry> entries = List.of(
                new DropZoneEntry(state.id()),
                new DropZoneEntry(state.id())
        );

        checker.compareEntries(entries, "DropZoneEntry", entryVersion());
    }

    // Schemas

    @Test
    public void newSchema() {
        EnumMap<Type, Integer> objectVersions = new EnumMap<>(CatalogObjectDescriptor.Type.class);
        objectVersions.put(Type.TABLE, entryVersion());
        objectVersions.put(Type.INDEX, entryVersion());
        objectVersions.put(Type.SYSTEM_VIEW, entryVersion());

        List<UpdateEntry> entries = TestSchemaDescriptors.schemas(state, entryVersion(), objectVersions)
                .stream()
                .map(NewSchemaEntry::new)
                .collect(Collectors.toList());

        checker.compareEntries(entries, "NewSchemaEntry", entryVersion());
    }

    @Test
    public void dropSchema() {
        List<UpdateEntry> entries = List.of(
                new DropSchemaEntry(state.id()),
                new DropSchemaEntry(state.id())
        );

        checker.compareEntries(entries, "DropSchemaEntry", entryVersion());
    }

    // Tables

    @Test
    public void newTable() {
        int version = entryVersion();

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

        checker.compareEntries(entries, "RenameTableEntry", entryVersion());
    }

    @Test
    public void dropTable() {
        List<UpdateEntry> entries = List.of(
                new DropTableEntry(state.id()),
                new DropTableEntry(state.id())
        );

        checker.compareEntries(entries, "DropTableEntry", entryVersion());
    }

    // Indexes

    @Test
    public void newIndex() {
        int version = entryVersion();

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

        checker.compareEntries(entries, "RenameIndexEntry", entryVersion());
    }

    @Test
    public void removeIndex() {
        List<UpdateEntry> entries = List.of(
                new RemoveIndexEntry(state.id()),
                new RemoveIndexEntry(state.id())
        );

        checker.compareEntries(entries, "RemoveIndexEntry", entryVersion());
    }

    @Test
    public void makeIndexAvailable() {
        List<UpdateEntry> entries = List.of(
                new MakeIndexAvailableEntry(state.id()),
                new MakeIndexAvailableEntry(state.id())
        );

        checker.compareEntries(entries, "MakeIndexAvailableEntry", entryVersion());
    }

    @Test
    public void startBuildingIndex() {
        List<UpdateEntry> entries = List.of(
                new StartBuildingIndexEntry(state.id()),
                new StartBuildingIndexEntry(state.id())
        );

        checker.compareEntries(entries, "StartBuildingIndexEntry", entryVersion());
    }

    @Test
    public void dropIndex() {
        List<UpdateEntry> entries = List.of(
                new DropIndexEntry(state.id()),
                new DropIndexEntry(state.id())
        );

        checker.compareEntries(entries, "DropIndexEntry", entryVersion());
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

        checker.compareEntries(entries, "NewColumnsEntry", entryVersion());
    }

    @Test
    public void alterColumn() {
        List<CatalogTableColumnDescriptor> columns = TestTableColumnDescriptors.columns(state);
        Collections.shuffle(columns, state.random());

        List<UpdateEntry> entries = List.of(
                new AlterColumnEntry(state.id(), columns.get(0)),
                new AlterColumnEntry(state.id(), columns.get(1))
        );
        checker.compareEntries(entries, "AlterColumnsEntry", entryVersion());
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
        checker.compareEntries(entries, "DropColumnsEntry", entryVersion());
    }

    // System views

    @Test
    public void newSystemView() {
        int version = entryVersion();

        List<UpdateEntry> entries = TestSystemViewDescriptors.systemViews(state, version)
                .stream()
                .map(NewSystemViewEntry::new)
                .collect(Collectors.toList());

        checker.compareEntries(entries, "NewSystemViewEntry", version);
    }

    protected int protocolVersion() {
        return 1;
    }

    protected int entryVersion() {
        return 1;
    }

    protected String dirName() {
        return "serialization_v1";
    }

    protected boolean expectExactVersion() {
        return false;
    }
}
