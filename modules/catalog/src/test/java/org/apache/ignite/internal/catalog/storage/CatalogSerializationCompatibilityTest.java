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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.serialization.UpdateLogMarshallerImpl;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.assertj.core.api.BDDAssertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for catalog storage objects. Protocol version 1.
 */
public class CatalogSerializationCompatibilityTest extends BaseIgniteAbstractTest {

    private final TestDescriptorState state = new TestDescriptorState(42);

    @Test
    public void snapshotEntry() {
        List<CatalogZoneDescriptor> zones = TestCatalogObjectDescriptors.zones(state);
        Catalog catalog1 = new Catalog(
                2367,
                5675344L,
                100,
                zones,
                TestCatalogObjectDescriptors.schemas(state),
                zones.get(0).id()
        );

        SnapshotEntry expectedEntry = new SnapshotEntry(catalog1);
        SnapshotEntry actualEntry = checkEntry(SnapshotEntry.class, "SnapshotEntry_1.bin");

        assertEquals(expectedEntry.typeId(), actualEntry.typeId());
        BDDAssertions.assertThat(expectedEntry.snapshot()).usingRecursiveComparison().isEqualTo(actualEntry.snapshot());
    }

    @Test
    public void snapshotEntryNoDefaultZone() {
        Catalog catalog1 = new Catalog(
                789879,
                23432L,
                2343,
                TestCatalogObjectDescriptors.zones(state),
                TestCatalogObjectDescriptors.schemas(state),
                null
        );

        SnapshotEntry expectedEntry = new SnapshotEntry(catalog1);
        SnapshotEntry actualEntry = checkEntry(SnapshotEntry.class, "SnapshotEntryNoDefaultZone_1.bin");

        assertEquals(expectedEntry.typeId(), actualEntry.typeId());
        BDDAssertions.assertThat(expectedEntry.snapshot()).usingRecursiveComparison().isEqualTo(actualEntry.snapshot());
    }

    @Test
    public void objectIdUpdate() {
        List<UpdateEntry> entries = List.of(new ObjectIdGenUpdateEntry(23431), new ObjectIdGenUpdateEntry(1204));
        List<UpdateEntry> actual = checkEntries(entries, "ObjectIdGenUpdateEntry_1.bin");

        assertEquals(entries.size(), actual.size());
        for (int i = 0; i < entries.size(); i++) {
            ObjectIdGenUpdateEntry expectedEntry = (ObjectIdGenUpdateEntry) entries.get(i);
            ObjectIdGenUpdateEntry actualEntry = (ObjectIdGenUpdateEntry) actual.get(i);

            BDDAssertions.assertThat(actualEntry).as("entry#" + i).usingRecursiveComparison().isEqualTo(expectedEntry);
        }
    }

    // Zones

    @Test
    public void newZone() {
        List<CatalogZoneDescriptor> zones = TestCatalogObjectDescriptors.zones(state);
        List<UpdateEntry> entries = zones.stream().map(NewZoneEntry::new).collect(Collectors.toList());
        List<UpdateEntry> actual = checkEntries(entries, "NewZoneEntry_1.bin");

        assertEquals(entries.size(), actual.size());
        for (int i = 0; i < entries.size(); i++) {
            NewZoneEntry expectedEntry = (NewZoneEntry) entries.get(i);
            NewZoneEntry actualEntry = (NewZoneEntry) actual.get(i);

            BDDAssertions.assertThat(actualEntry).as("entry#" + i).usingRecursiveComparison().isEqualTo(expectedEntry);
        }
    }

    @Test
    public void alterZone() {
        List<CatalogZoneDescriptor> zones = TestCatalogObjectDescriptors.zones(state);
        List<UpdateEntry> entries = List.of(
                new AlterZoneEntry(zones.get(1)),
                new AlterZoneEntry(zones.get(2))
        );

        List<UpdateEntry> actual = checkEntries(entries, "AlterZoneEntry_1.bin");
        assertEquals(entries.size(), actual.size());
        for (int i = 0; i < entries.size(); i++) {
            AlterZoneEntry expectedEntry = (AlterZoneEntry) entries.get(i);
            AlterZoneEntry actualEntry = (AlterZoneEntry) actual.get(i);

            BDDAssertions.assertThat(actualEntry).as("entry#" + i).usingRecursiveComparison().isEqualTo(expectedEntry);
        }
    }

    @Test
    public void setDefaultZone() {
        List<UpdateEntry> entries = List.of(
                new SetDefaultZoneEntry(state.id()),
                new SetDefaultZoneEntry(state.id())
        );
        List<UpdateEntry> actual = checkEntries(entries, "SetDefaultZoneEntry_1.bin");

        assertEquals(entries.size(), actual.size());
        for (int i = 0; i < entries.size(); i++) {
            SetDefaultZoneEntry expectedEntry = (SetDefaultZoneEntry) entries.get(i);
            SetDefaultZoneEntry actualEntry = (SetDefaultZoneEntry) actual.get(i);

            BDDAssertions.assertThat(actualEntry).as("entry#" + i).usingRecursiveComparison().isEqualTo(expectedEntry);
        }
    }

    @Test
    public void dropZone() {
        List<UpdateEntry> entries = List.of(
                new DropZoneEntry(state.id()),
                new DropZoneEntry(state.id())
        );

        List<UpdateEntry> actual = checkEntries(entries, "DropZoneEntry_1.bin");
        assertEquals(entries.size(), actual.size());

        for (int i = 0; i < entries.size(); i++) {
            DropZoneEntry expectedEntry = (DropZoneEntry) entries.get(i);
            DropZoneEntry actualEntry = (DropZoneEntry) actual.get(i);

            BDDAssertions.assertThat(actualEntry).as("entry#" + i).usingRecursiveComparison().isEqualTo(expectedEntry);
        }
    }

    // Schemas

    @Test
    public void newSchema() {
        List<UpdateEntry> entries = TestCatalogObjectDescriptors.schemas(state)
                .stream()
                .map(NewSchemaEntry::new)
                .collect(Collectors.toList());

        List<UpdateEntry> actual = checkEntries(entries, "NewSchemaEntry_1.bin");
        assertEquals(entries.size(), actual.size());

        for (int i = 0; i < entries.size(); i++) {
            NewSchemaEntry expectedEntry = (NewSchemaEntry) entries.get(i);
            NewSchemaEntry actualEntry = (NewSchemaEntry) actual.get(i);

            BDDAssertions.assertThat(actualEntry).as("entry#" + i).usingRecursiveComparison().isEqualTo(expectedEntry);
        }
    }

    @Test
    public void dropSchema() {
        List<UpdateEntry> entries = List.of(
                new DropSchemaEntry(state.id()),
                new DropSchemaEntry(state.id())
        );

        List<UpdateEntry> actual = checkEntries(entries, "DropSchemaEntry_1.bin");
        assertEquals(entries.size(), actual.size());

        for (int i = 0; i < entries.size(); i++) {
            DropSchemaEntry expectedEntry = (DropSchemaEntry) entries.get(i);
            DropSchemaEntry actualEntry = (DropSchemaEntry) actual.get(i);

            BDDAssertions.assertThat(actualEntry).as("entry#" + i).usingRecursiveComparison().isEqualTo(expectedEntry);
        }
    }

    // Tables

    @Test
    public void newTable() {
        List<UpdateEntry> entries = TestCatalogObjectDescriptors.tables(state)
                .stream()
                .map(NewTableEntry::new)
                .collect(Collectors.toList());

        List<UpdateEntry> actual = checkEntries(entries, "NewTableEntry_1.bin");
        assertEquals(entries.size(), actual.size());

        for (int i = 0; i < entries.size(); i++) {
            NewTableEntry expectedEntry = (NewTableEntry) entries.get(i);
            NewTableEntry actualEntry = (NewTableEntry) actual.get(i);

            BDDAssertions.assertThat(actualEntry).as("entry#" + i).usingRecursiveComparison().isEqualTo(expectedEntry);
        }
    }

    @Test
    public void renameTable() {
        List<UpdateEntry> entries = List.of(
                new RenameTableEntry(state.id(), "NEW_NAME1"),
                new RenameTableEntry(state.id(), "NEW_NAME2")
        );

        List<UpdateEntry> actual = checkEntries(entries, "RenameTableEntry_1.bin");
        assertEquals(entries.size(), actual.size());

        for (int i = 0; i < entries.size(); i++) {
            RenameTableEntry expectedEntry = (RenameTableEntry) entries.get(i);
            RenameTableEntry actualEntry = (RenameTableEntry) actual.get(i);

            BDDAssertions.assertThat(actualEntry).as("entry#" + i).usingRecursiveComparison().isEqualTo(expectedEntry);
        }
    }

    @Test
    public void dropTable() {
        List<UpdateEntry> entries = List.of(
                new DropTableEntry(state.id()),
                new DropTableEntry(state.id())
        );

        List<UpdateEntry> actual = checkEntries(entries, "DropTableEntry_1.bin");
        assertEquals(entries.size(), actual.size());

        for (int i = 0; i < entries.size(); i++) {
            DropTableEntry expectedEntry = (DropTableEntry) entries.get(i);
            DropTableEntry actualEntry = (DropTableEntry) actual.get(i);

            BDDAssertions.assertThat(actualEntry).as("entry#" + i).usingRecursiveComparison().isEqualTo(expectedEntry);
        }
    }

    // Columns

    // Indexes

    @Test
    public void newIndex() {
        List<UpdateEntry> entries1 = TestCatalogObjectDescriptors.sortedIndices(state)
                .stream()
                .map(NewIndexEntry::new)
                .collect(Collectors.toList());

        List<UpdateEntry> entries2 = TestCatalogObjectDescriptors.hashIndices(state)
                .stream()
                .map(NewIndexEntry::new)
                .collect(Collectors.toList());

        List<UpdateEntry> entries = new ArrayList<>(entries1);
        entries.addAll(entries2);

        Collections.shuffle(entries, state.random());

        List<UpdateEntry> actual = checkEntries(entries, "NewIndexEntry_1.bin");
        assertEquals(entries.size(), actual.size());

        for (int i = 0; i < entries.size(); i++) {
            NewIndexEntry expectedEntry = (NewIndexEntry) entries.get(i);
            NewIndexEntry actualEntry = (NewIndexEntry) actual.get(i);

            BDDAssertions.assertThat(actualEntry).as("entry#" + i).usingRecursiveComparison().isEqualTo(expectedEntry);
        }
    }

    @Test
    public void renameIndex() {
        List<UpdateEntry> entries = List.of(new RenameIndexEntry(state.id(), "NEW_NAME"));

        List<UpdateEntry> actual = checkEntries(entries, "RenameIndexEntry_1.bin");
        assertEquals(entries.size(), actual.size());

        for (int i = 0; i < entries.size(); i++) {
            RenameIndexEntry expectedEntry = (RenameIndexEntry) entries.get(i);
            RenameIndexEntry actualEntry = (RenameIndexEntry) actual.get(i);

            BDDAssertions.assertThat(actualEntry).as("entry#" + i).usingRecursiveComparison().isEqualTo(expectedEntry);
        }
    }

    @Test
    public void removeIndex() {
        List<UpdateEntry> entries = List.of(
                new RemoveIndexEntry(state.id()),
                new RemoveIndexEntry(state.id())
        );

        List<UpdateEntry> actual = checkEntries(entries, "RemoveIndexEntry_1.bin");
        assertEquals(entries.size(), actual.size());

        for (int i = 0; i < entries.size(); i++) {
            RemoveIndexEntry expectedEntry = (RemoveIndexEntry) entries.get(i);
            RemoveIndexEntry actualEntry = (RemoveIndexEntry) actual.get(i);

            BDDAssertions.assertThat(actualEntry).as("entry#" + i).usingRecursiveComparison().isEqualTo(expectedEntry);
        }
    }

    @Test
    public void makeIndexAvailable() {
        List<UpdateEntry> entries = List.of(
                new MakeIndexAvailableEntry(state.id()),
                new MakeIndexAvailableEntry(state.id())
        );

        List<UpdateEntry> actual = checkEntries(entries, "MakeIndexAvailableEntry_1.bin");
        assertEquals(entries.size(), actual.size());

        for (int i = 0; i < entries.size(); i++) {
            MakeIndexAvailableEntry expectedEntry = (MakeIndexAvailableEntry) entries.get(i);
            MakeIndexAvailableEntry actualEntry = (MakeIndexAvailableEntry) actual.get(i);

            BDDAssertions.assertThat(actualEntry).as("entry#" + i).usingRecursiveComparison().isEqualTo(expectedEntry);
        }
    }

    @Test
    public void startBuildingIndex() {
        List<UpdateEntry> entries = List.of(
                new StartBuildingIndexEntry(state.id()),
                new StartBuildingIndexEntry(state.id())
        );

        List<UpdateEntry> actual = checkEntries(entries, "StartBuildingIndexEntry_1.bin");
        assertEquals(entries.size(), actual.size());

        for (int i = 0; i < entries.size(); i++) {
            StartBuildingIndexEntry expectedEntry = (StartBuildingIndexEntry) entries.get(i);
            StartBuildingIndexEntry actualEntry = (StartBuildingIndexEntry) actual.get(i);

            BDDAssertions.assertThat(actualEntry).as("entry#" + i).usingRecursiveComparison().isEqualTo(expectedEntry);
        }
    }

    @Test
    public void dropIndex() {
        List<UpdateEntry> entries = List.of(
                new DropIndexEntry(state.id()),
                new DropIndexEntry(state.id())
        );

        List<UpdateEntry> actual = checkEntries(entries, "DropIndexEntry_1.bin");
        assertEquals(entries.size(), actual.size());

        for (int i = 0; i < entries.size(); i++) {
            DropIndexEntry expectedEntry = (DropIndexEntry) entries.get(i);
            DropIndexEntry actualEntry = (DropIndexEntry) actual.get(i);

            BDDAssertions.assertThat(actualEntry).as("entry#" + i).usingRecursiveComparison().isEqualTo(expectedEntry);
        }
    }

    // Columns

    @Test
    public void newColumns() {
        List<CatalogTableColumnDescriptor> columns1 = TestCatalogObjectDescriptors.columns(state);
        List<CatalogTableColumnDescriptor> columns2 = TestCatalogObjectDescriptors.columns(state);

        Collections.shuffle(columns1, state.random());
        Collections.shuffle(columns2, state.random());

        List<UpdateEntry> entries = List.of(
                new NewColumnsEntry(state.id(), columns1),
                new NewColumnsEntry(state.id(), columns2)
        );

        List<UpdateEntry> actual = checkEntries(entries, "NewColumnsEntry_1.bin");
        assertEquals(entries.size(), actual.size());

        for (int i = 0; i < entries.size(); i++) {
            NewColumnsEntry expectedEntry = (NewColumnsEntry) entries.get(i);
            NewColumnsEntry actualEntry = (NewColumnsEntry) actual.get(i);

            BDDAssertions.assertThat(actualEntry).as("entry#" + i).usingRecursiveComparison().isEqualTo(expectedEntry);
        }
    }

    @Test
    public void alterColumn() {
        List<CatalogTableColumnDescriptor> columns = TestCatalogObjectDescriptors.columns(state);
        Collections.shuffle(columns, state.random());

        List<UpdateEntry> entries = List.of(
                new AlterColumnEntry(state.id(), columns.get(0)),
                new AlterColumnEntry(state.id(), columns.get(1))
        );

        List<UpdateEntry> actual = checkEntries(entries, "AlterColumnsEntry_1.bin");
        assertEquals(entries.size(), actual.size());

        for (int i = 0; i < entries.size(); i++) {
            AlterColumnEntry expectedEntry = (AlterColumnEntry) entries.get(i);
            AlterColumnEntry actualEntry = (AlterColumnEntry) actual.get(i);

            BDDAssertions.assertThat(actualEntry).as("entry#" + i).usingRecursiveComparison().isEqualTo(expectedEntry);
        }
    }

    @Test
    public void dropColumns() {
        List<CatalogTableColumnDescriptor> columns1 = TestCatalogObjectDescriptors.columns(state);
        List<CatalogTableColumnDescriptor> columns2 = TestCatalogObjectDescriptors.columns(state);

        Collections.shuffle(columns1, state.random());
        Collections.shuffle(columns2, state.random());

        List<UpdateEntry> entries = List.of(
                new DropColumnsEntry(state.id(), Set.of("C1", "C2")),
                new DropColumnsEntry(state.id(), Set.of("C3"))
        );

        List<UpdateEntry> actual = checkEntries(entries, "DropColumnsEntry_1.bin");
        assertEquals(entries.size(), actual.size());

        for (int i = 0; i < entries.size(); i++) {
            DropColumnsEntry expectedEntry = (DropColumnsEntry) entries.get(i);
            DropColumnsEntry actualEntry = (DropColumnsEntry) actual.get(i);

            BDDAssertions.assertThat(actualEntry).as("entry#" + i).usingRecursiveComparison().isEqualTo(expectedEntry);
        }
    }

    // System views

    @Test
    public void newSystemView() {
        List<UpdateEntry> entries = TestCatalogObjectDescriptors.systemViews(state)
                .stream()
                .map(NewSystemViewEntry::new)
                .collect(Collectors.toList());

        List<UpdateEntry> actual = checkEntries(entries, "NewSystemViewEntry_1.bin");
        assertEquals(entries.size(), actual.size());

        for (int i = 0; i < entries.size(); i++) {
            NewSystemViewEntry expectedEntry = (NewSystemViewEntry) entries.get(i);
            NewSystemViewEntry actualEntry = (NewSystemViewEntry) actual.get(i);

            BDDAssertions.assertThat(actualEntry).as("entry#" + i).usingRecursiveComparison().isEqualTo(expectedEntry);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <T extends UpdateEntry> List<T> checkEntries(List<T> entries, String fileName) {
        VersionedUpdate update = new VersionedUpdate(1, 100L, (List<UpdateEntry>) entries);
        VersionedUpdate deserializedUpdate = checkEntry(VersionedUpdate.class, fileName);

        assertEquals(update.version(), deserializedUpdate.version());
        assertEquals(update.typeId(), deserializedUpdate.typeId());
        assertEquals(update.delayDurationMs(), deserializedUpdate.delayDurationMs());

        return (List) deserializedUpdate.entries();
    }

    private <T extends UpdateLogEvent> T checkEntry(Class<T> entryClass, String fileName) {
        String resourceName = "serialization_v1/" + fileName;

        UpdateLogMarshallerImpl marshaller = new UpdateLogMarshallerImpl();

        byte[] srcBytes;

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourceName)) {
            assertNotNull(is, "Resource does not exist: " + resourceName);
            while (is.available() > 0) {
                bos.write(is.read());
            }
            srcBytes = bos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to resource", e);
        }

        return entryClass.cast(marshaller.unmarshall(srcBytes));
    }
}
