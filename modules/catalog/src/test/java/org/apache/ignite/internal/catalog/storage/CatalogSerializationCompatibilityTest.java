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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogEntrySerializerProvider;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntry;
import org.apache.ignite.internal.catalog.storage.serialization.UpdateLogMarshaller;
import org.apache.ignite.internal.catalog.storage.serialization.UpdateLogMarshallerImpl;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.assertj.core.api.BDDAssertions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for catalog storage objects. Protocol version 1.
 */
public class CatalogSerializationCompatibilityTest extends BaseIgniteAbstractTest {
    private static final String UPDATE_TIMESTAMP_FIELD_REGEX = ".*updateTimestamp";

    final TestDescriptorState state = new TestDescriptorState(42);

    @Test
    public void snapshotEntry() {
        List<CatalogZoneDescriptor> zones = zones();
        Catalog catalog1 = new Catalog(
                2367,
                5675344L,
                100,
                zones,
                TestCatalogObjectDescriptors.schemas(state),
                zones.get(0).id()
        );

        SnapshotEntry snapshotEntry = new SnapshotEntry(catalog1);

        compareSnapshotEntry(snapshotEntry, "SnapshotEntry", entryVersion());
    }

    @Test
    public void snapshotEntryNoDefaultZone() {
        Catalog catalog1 = new Catalog(
                789879,
                23432L,
                2343,
                zones(),
                TestCatalogObjectDescriptors.schemas(state),
                null
        );

        SnapshotEntry snapshotEntry = new SnapshotEntry(catalog1);

        compareSnapshotEntry(snapshotEntry, "SnapshotEntryNoDefaultZone", entryVersion());
    }

    @Test
    public void objectIdUpdate() {
        List<UpdateEntry> entries = List.of(new ObjectIdGenUpdateEntry(23431), new ObjectIdGenUpdateEntry(1204));

        compareEntries(entries, "ObjectIdGenUpdateEntry", entryVersion());
    }

    // Zones

    @Test
    public void newZone() {
        List<CatalogZoneDescriptor> zones = zones();
        List<UpdateEntry> entries = zones.stream().map(NewZoneEntry::new).collect(Collectors.toList());

        compareEntries(entries, "NewZoneEntry", entryVersion());
    }

    @Test
    public void alterZone() {
        List<CatalogZoneDescriptor> zones = zones();
        List<UpdateEntry> entries = List.of(
                new AlterZoneEntry(zones.get(1)),
                new AlterZoneEntry(zones.get(2))
        );

        compareEntries(entries, "AlterZoneEntry", entryVersion());
    }

    @Test
    public void setDefaultZone() {
        List<UpdateEntry> entries = List.of(
                new SetDefaultZoneEntry(state.id()),
                new SetDefaultZoneEntry(state.id())
        );

        compareEntries(entries, "SetDefaultZoneEntry", entryVersion());
    }

    @Test
    public void dropZone() {
        List<UpdateEntry> entries = List.of(
                new DropZoneEntry(state.id()),
                new DropZoneEntry(state.id())
        );

        compareEntries(entries, "DropZoneEntry", entryVersion());
    }

    // Schemas

    @Test
    public void newSchema() {
        List<UpdateEntry> entries = TestCatalogObjectDescriptors.schemas(state)
                .stream()
                .map(NewSchemaEntry::new)
                .collect(Collectors.toList());

        compareEntries(entries, "NewSchemaEntry", entryVersion());
    }

    @Test
    public void dropSchema() {
        List<UpdateEntry> entries = List.of(
                new DropSchemaEntry(state.id()),
                new DropSchemaEntry(state.id())
        );

        compareEntries(entries, "DropSchemaEntry", entryVersion());
    }

    // Tables

    @Test
    public void newTable() {
        List<UpdateEntry> entries = TestCatalogObjectDescriptors.tables(state)
                .stream()
                .map(NewTableEntry::new)
                .collect(Collectors.toList());

        compareEntries(entries, "NewTableEntry", entryVersion());
    }

    @Test
    public void renameTable() {
        List<UpdateEntry> entries = List.of(
                new RenameTableEntry(state.id(), "NEW_NAME1"),
                new RenameTableEntry(state.id(), "NEW_NAME2")
        );

        compareEntries(entries, "RenameTableEntry", entryVersion());
    }

    @Test
    public void dropTable() {
        List<UpdateEntry> entries = List.of(
                new DropTableEntry(state.id()),
                new DropTableEntry(state.id())
        );

        compareEntries(entries, "DropTableEntry", entryVersion());
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

        compareEntries(entries, "NewIndexEntry", entryVersion());
    }

    @Test
    public void renameIndex() {
        List<UpdateEntry> entries = List.of(new RenameIndexEntry(state.id(), "NEW_NAME"));

        compareEntries(entries, "RenameIndexEntry", entryVersion());
    }

    @Test
    public void removeIndex() {
        List<UpdateEntry> entries = List.of(
                new RemoveIndexEntry(state.id()),
                new RemoveIndexEntry(state.id())
        );

        compareEntries(entries, "RemoveIndexEntry", entryVersion());
    }

    @Test
    public void makeIndexAvailable() {
        List<UpdateEntry> entries = List.of(
                new MakeIndexAvailableEntry(state.id()),
                new MakeIndexAvailableEntry(state.id())
        );

        compareEntries(entries, "MakeIndexAvailableEntry", entryVersion());
    }

    @Test
    public void startBuildingIndex() {
        List<UpdateEntry> entries = List.of(
                new StartBuildingIndexEntry(state.id()),
                new StartBuildingIndexEntry(state.id())
        );

        compareEntries(entries, "StartBuildingIndexEntry", entryVersion());
    }

    @Test
    public void dropIndex() {
        List<UpdateEntry> entries = List.of(
                new DropIndexEntry(state.id()),
                new DropIndexEntry(state.id())
        );

        compareEntries(entries, "DropIndexEntry", entryVersion());
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

        compareEntries(entries, "NewColumnsEntry", entryVersion());
    }

    @Test
    public void alterColumn() {
        List<CatalogTableColumnDescriptor> columns = TestCatalogObjectDescriptors.columns(state);
        Collections.shuffle(columns, state.random());

        List<UpdateEntry> entries = List.of(
                new AlterColumnEntry(state.id(), columns.get(0)),
                new AlterColumnEntry(state.id(), columns.get(1))
        );
        compareEntries(entries, "AlterColumnsEntry", entryVersion());
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
        compareEntries(entries, "DropColumnsEntry", entryVersion());
    }

    // System views

    @Test
    public void newSystemView() {
        List<UpdateEntry> entries = TestCatalogObjectDescriptors.systemViews(state)
                .stream()
                .map(NewSystemViewEntry::new)
                .collect(Collectors.toList());

        compareEntries(entries, "NewSystemViewEntry", entryVersion());
    }

    protected void compareSnapshotEntry(SnapshotEntry expectedEntry, String fileName, int version) {
        SnapshotEntry actualEntry = checkEntry(SnapshotEntry.class, fileName, version, expectedEntry);

        assertEquals(expectedEntry.typeId(), actualEntry.typeId());
        assertEquals(expectedEntry.activationTime(), actualEntry.activationTime(), "activationTime");
        assertEquals(expectedEntry.objectIdGenState(), actualEntry.objectIdGenState(), "objectIdGenState");
        assertEquals(expectedEntry.defaultZoneId(), actualEntry.defaultZoneId(), "defaultZoneId");

        var assertion = BDDAssertions.assertThat(expectedEntry.snapshot())
                .usingRecursiveComparison();

        if (entryVersion() == 1) {
            // Ignoring update timestamp for version 1.
            assertion = assertion.ignoringFieldsMatchingRegexes(UPDATE_TIMESTAMP_FIELD_REGEX);
        }

        assertion.isEqualTo(actualEntry.snapshot());
    }

    protected void compareEntries(List<UpdateEntry> entries, String fileName, int version) {
        List<UpdateEntry> actual = checkEntries(entries, fileName, version);
        assertEquals(entries.size(), actual.size());

        for (int i = 0; i < entries.size(); i++) {
            UpdateEntry expectedEntry = entries.get(i);
            UpdateEntry actualEntry = actual.get(i);

            var assertion = BDDAssertions.assertThat(actualEntry).as("entry#" + i)
                    .usingRecursiveComparison();

            if (entryVersion() == 1) {
                // Ignoring update timestamp for version 1.
                assertion = assertion.ignoringFieldsMatchingRegexes(UPDATE_TIMESTAMP_FIELD_REGEX);
            }

            assertion.isEqualTo(expectedEntry);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <T extends UpdateEntry> List<T> checkEntries(List<? extends T> entries, String fileName, int version) {
        VersionedUpdate update = new VersionedUpdate(1, 100L, (List<UpdateEntry>) entries);
        VersionedUpdate deserializedUpdate = checkEntry(VersionedUpdate.class, fileName, version, update);

        assertEquals(update.version(), deserializedUpdate.version());
        assertEquals(update.typeId(), deserializedUpdate.typeId());
        assertEquals(update.delayDurationMs(), deserializedUpdate.delayDurationMs());

        return (List) deserializedUpdate.entries();
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

    protected List<CatalogZoneDescriptor> zones() {
        return TestCatalogObjectDescriptors.zonesWithDefaultQuorumSize(state);
    }

    private <T extends UpdateLogEvent> T checkEntry(Class<T> entryClass, String entryFileName, int version, UpdateLogEvent entry) {
        String fileName = format("{}_{}.bin", entryFileName, version);
        String resourceName = dirName() + "/" + fileName;

        CatalogEntrySerializerProvider provider;
        if (expectExactVersion()) {
            provider = new VersionCheckingProvider(protocolVersion());
        } else {
            provider = CatalogEntrySerializerProvider.DEFAULT_PROVIDER;
        }

        log.info("Read fileName: {}, class: {}, version: {}", fileName, entryClass.getSimpleName(), version);

        UpdateLogMarshaller marshaller = new UpdateLogMarshallerImpl(provider, protocolVersion());

        // Uncomment this and run tests with corresponding entryVersion to write entry to file
        // writeEntry(entry, resourceName, marshaller);

        byte[] srcBytes;

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourceName)) {
            assertNotNull(is, "Resource does not exist: " + resourceName);
            while (is.available() > 0) {
                bos.write(is.read());
            }
            srcBytes = bos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read resource", e);
        }

        return entryClass.cast(marshaller.unmarshall(srcBytes));
    }

    @SuppressWarnings("unused")
    private static void writeEntry(UpdateLogEvent entry, String resourceName, UpdateLogMarshaller marshaller) {
        try {
            Path resourcePath = Path.of(resourceName);
            Files.createDirectories(resourcePath.getParent());
            Files.write(resourcePath, marshaller.marshall(entry));
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to write resource", e);
        }
    }

    private static class VersionCheckingProvider implements CatalogEntrySerializerProvider {

        private final CatalogEntrySerializerProvider provider;

        private final int expected;

        private VersionCheckingProvider(int expected) {
            this.provider = DEFAULT_PROVIDER;
            this.expected = expected;
        }

        @Override
        public <T extends MarshallableEntry> CatalogObjectSerializer<T> get(int version, int typeId) {
            CatalogObjectSerializer<MarshallableEntry> serializer = provider.get(version, typeId);

            checkVersion(typeId, version);

            return (CatalogObjectSerializer<T>) serializer;
        }

        @Override
        public int latestSerializerVersion(int typeId) {
            int latest = provider.latestSerializerVersion(typeId);
            checkVersion(typeId, latest);
            return latest;
        }

        private void checkVersion(int typeId, int version) {
            if (version != expected) {
                Assertions.fail("Requested unexpected version for type " + typeId + ". All versions must be " + expected);
            }
        }
    }
}
