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

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfileDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfilesDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor.SystemViewType;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.catalog.storage.serialization.UpdateLogMarshallerImpl;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.sql.ColumnType;
import org.assertj.core.api.BDDAssertions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

/**
 * Tests to verify catalog storage entries serialization.
 */
public class CatalogEntrySerializationTest extends BaseIgniteAbstractTest {
    private final UpdateLogMarshallerImpl marshaller = new UpdateLogMarshallerImpl();

    @ParameterizedTest
    @EnumSource(value = MarshallableEntryType.class, names = "VERSIONED_UPDATE", mode = Mode.EXCLUDE)
    void test(MarshallableEntryType type) {
        switch (type) {
            case ALTER_COLUMN:
                alterColumnEntry();
                break;

            case ALTER_ZONE:
                alterZoneEntry();
                break;

            case NEW_ZONE:
                newZoneEntry();
                break;

            case DROP_COLUMN:
                dropColumnsEntry();
                break;

            case DROP_INDEX:
                dropIndexEntry();
                break;

            case DROP_TABLE:
                dropTableEntry();
                break;

            case DROP_ZONE:
                dropZoneEntry();
                break;

            case MAKE_INDEX_AVAILABLE:
                makeIndexAvailableEntry();
                break;

            case REMOVE_INDEX:
                removeIndexEntry();
                break;

            case START_BUILDING_INDEX:
                startBuildingIndexEntry();
                break;

            case NEW_COLUMN:
                newColumnsEntry();
                break;

            case NEW_INDEX:
                newIndexEntry();
                break;

            case NEW_SYS_VIEW:
                newSystemViewEntry();
                break;

            case NEW_TABLE:
                newTableEntry();
                break;

            case RENAME_TABLE:
                renameTableEntry();
                break;

            case ID_GENERATOR:
                objectIdGenUpdateEntry();
                break;

            case SNAPSHOT:
                snapshotEntry();
                break;

            case RENAME_INDEX:
                renameIndexEntry();
                break;

            default:
                throw new UnsupportedOperationException("Test not implemented " + type);
        }
    }

    private void alterZoneEntry() {
        CatalogStorageProfilesDescriptor profiles =
                new CatalogStorageProfilesDescriptor(List.of(new CatalogStorageProfileDescriptor("default")));
        UpdateEntry entry1 = new AlterZoneEntry(newCatalogZoneDescriptor("zone1", profiles));

        VersionedUpdate update = newVersionedUpdate(entry1, entry1);

        assertVersionedUpdate(update, serialize(update));
    }

    private void newZoneEntry() {
        CatalogStorageProfilesDescriptor profiles =
                new CatalogStorageProfilesDescriptor(List.of(new CatalogStorageProfileDescriptor("default")));

        UpdateEntry entry1 = new NewZoneEntry(newCatalogZoneDescriptor("zone1", profiles));
        UpdateEntry entry2 = new NewZoneEntry(newCatalogZoneDescriptor("zone2", profiles));
        VersionedUpdate update = newVersionedUpdate(entry1, entry2);

        assertVersionedUpdate(update, serialize(update));
    }

    private void alterColumnEntry() {
        CatalogTableColumnDescriptor desc1 = newCatalogTableColumnDescriptor("c0", null);
        CatalogTableColumnDescriptor desc2 =
                newCatalogTableColumnDescriptor("c1", DefaultValue.constant(new CustomDefaultValue(Integer.MAX_VALUE)));
        CatalogTableColumnDescriptor desc3 =
                newCatalogTableColumnDescriptor("c2", DefaultValue.functionCall("function"));
        CatalogTableColumnDescriptor desc4 = newCatalogTableColumnDescriptor("c3", DefaultValue.constant(null));

        UpdateEntry entry1 = new AlterColumnEntry(1, desc1, "public");
        UpdateEntry entry2 = new AlterColumnEntry(1, desc2, "public");
        UpdateEntry entry3 = new AlterColumnEntry(1, desc3, "public");
        UpdateEntry entry4 = new AlterColumnEntry(1, desc4, "public");

        VersionedUpdate update = newVersionedUpdate(entry1, entry2, entry3, entry4);

        assertVersionedUpdate(update, serialize(update));
    }

    private void dropColumnsEntry() {
        DropColumnsEntry entry = new DropColumnsEntry(1, Set.of("C1", "C2"), "PUBLIC");

        VersionedUpdate update = newVersionedUpdate(entry);

        assertVersionedUpdate(update, serialize(update));
    }

    private void dropIndexEntry() {
        DropIndexEntry entry = new DropIndexEntry(231, 23);

        VersionedUpdate update = newVersionedUpdate(entry);

        assertVersionedUpdate(update, serialize(update));
    }

    private void removeIndexEntry() {
        RemoveIndexEntry entry = new RemoveIndexEntry(231);

        VersionedUpdate update = newVersionedUpdate(entry);

        assertVersionedUpdate(update, serialize(update));
    }

    private void dropTableEntry() {
        DropTableEntry entry = new DropTableEntry(23, "PUBLIC");

        VersionedUpdate update = newVersionedUpdate(entry);

        assertVersionedUpdate(update, serialize(update));
    }

    private void dropZoneEntry() {
        DropZoneEntry entry = new DropZoneEntry(1);

        VersionedUpdate update = newVersionedUpdate(entry);

        assertVersionedUpdate(update, serialize(update));
    }

    private void makeIndexAvailableEntry() {
        MakeIndexAvailableEntry entry = new MakeIndexAvailableEntry(321);

        VersionedUpdate update = newVersionedUpdate(entry);

        assertVersionedUpdate(update, serialize(update));
    }

    private void startBuildingIndexEntry() {
        StartBuildingIndexEntry entry = new StartBuildingIndexEntry(321);

        VersionedUpdate update = newVersionedUpdate(entry);

        assertVersionedUpdate(update, serialize(update));
    }

    private void newColumnsEntry() {
        CatalogTableColumnDescriptor columnDescriptor1 = newCatalogTableColumnDescriptor("c1", DefaultValue.constant(null));
        CatalogTableColumnDescriptor columnDescriptor2 = newCatalogTableColumnDescriptor("c2", DefaultValue.functionCall("func"));

        NewColumnsEntry entry = new NewColumnsEntry(11, List.of(columnDescriptor1, columnDescriptor2), "PUBLIC");

        VersionedUpdate update = newVersionedUpdate(entry);

        assertVersionedUpdate(update, serialize(update));
    }

    private void newIndexEntry() {
        CatalogSortedIndexDescriptor sortedIndexDescriptor = newSortedIndexDescriptor("idx1");
        CatalogHashIndexDescriptor hashIndexDescriptor = newHashIndexDescriptor("idx2");

        NewIndexEntry sortedIdxEntry = new NewIndexEntry(sortedIndexDescriptor, "PUBLIC");
        NewIndexEntry hashIdxEntry = new NewIndexEntry(hashIndexDescriptor, "PUBLIC");

        VersionedUpdate update = newVersionedUpdate(sortedIdxEntry, hashIdxEntry);

        assertVersionedUpdate(update, serialize(update));
    }

    private void newTableEntry() {
        CatalogTableColumnDescriptor col1 = newCatalogTableColumnDescriptor("c0", null);
        CatalogTableColumnDescriptor col2 = newCatalogTableColumnDescriptor("c1", null);
        CatalogTableColumnDescriptor col3 = newCatalogTableColumnDescriptor("c3", null);
        CatalogTableColumnDescriptor col4 = newCatalogTableColumnDescriptor("c2", null);

        List<CatalogTableColumnDescriptor> columns = List.of(col1, col2, col3, col4);

        NewTableEntry entry1 = new NewTableEntry(newTableDescriptor("Table1", columns, List.of("c1", "c2"), null), "PUBLIC");
        NewTableEntry entry2 = new NewTableEntry(newTableDescriptor("Table1", columns, List.of("c1", "c2"), List.of()), "PUBLIC");
        NewTableEntry entry3 = new NewTableEntry(newTableDescriptor("Table1", columns, List.of("c1", "c2"), List.of("c2")), "PUBLIC");
        NewTableEntry entry4 = new NewTableEntry(newTableDescriptor("Table1", columns, List.of("c1", "c2"), List.of("c1")), "PUBLIC");

        VersionedUpdate update = newVersionedUpdate(entry1, entry2, entry3, entry4);
        VersionedUpdate deserialized = serialize(update);

        assertVersionedUpdate(update, deserialized);

        NewTableEntry deserializedEntry = (NewTableEntry) deserialized.entries().get(0);
        assertSame(deserializedEntry.descriptor().primaryKeyColumns(), deserializedEntry.descriptor().colocationColumns());
    }

    private void newSystemViewEntry() {
        CatalogTableColumnDescriptor col1 = newCatalogTableColumnDescriptor("c1", null);
        CatalogTableColumnDescriptor col2 = newCatalogTableColumnDescriptor("c2", null);

        CatalogSystemViewDescriptor nodeDesc =
                new CatalogSystemViewDescriptor(1, "view1", List.of(col1, col2), SystemViewType.NODE);
        CatalogSystemViewDescriptor clusterDesc =
                new CatalogSystemViewDescriptor(1, "view1", List.of(col1, col2), SystemViewType.CLUSTER);

        NewSystemViewEntry nodeEntry = new NewSystemViewEntry(nodeDesc, "PUBLIC");
        NewSystemViewEntry clusterEntry = new NewSystemViewEntry(clusterDesc, "PUBLIC");

        VersionedUpdate update = newVersionedUpdate(nodeEntry, clusterEntry);

        assertVersionedUpdate(update, serialize(update));
    }

    private void renameTableEntry() {
        RenameTableEntry entry = new RenameTableEntry(1, "newName");

        VersionedUpdate update = newVersionedUpdate(entry);

        assertVersionedUpdate(update, serialize(update));
    }

    private void objectIdGenUpdateEntry() {
        ObjectIdGenUpdateEntry entry = new ObjectIdGenUpdateEntry(Integer.MAX_VALUE);

        VersionedUpdate update = newVersionedUpdate(entry);

        assertVersionedUpdate(update, serialize(update));
    }

    private void snapshotEntry() {
        CatalogTableColumnDescriptor col1 = newCatalogTableColumnDescriptor("c1", null);
        CatalogTableColumnDescriptor col2 = newCatalogTableColumnDescriptor("c2", null);

        List<CatalogTableColumnDescriptor> columns = List.of(col1, col2);

        CatalogTableDescriptor[] tables = {
                newTableDescriptor("Table1", columns),
                newTableDescriptor("Table2", columns)
        };

        CatalogIndexDescriptor[] indexes = {
                newSortedIndexDescriptor("idx1"),
                newHashIndexDescriptor("idx2")
        };

        CatalogSystemViewDescriptor[] views = {
                new CatalogSystemViewDescriptor(1, "view1", columns, SystemViewType.NODE),
                new CatalogSystemViewDescriptor(1, "view2", columns, SystemViewType.CLUSTER)
        };

        CatalogStorageProfilesDescriptor profiles =
                new CatalogStorageProfilesDescriptor(List.of(new CatalogStorageProfileDescriptor("default")));

        SnapshotEntry entry = new SnapshotEntry(new Catalog(2, 0L, 1,
                List.of(newCatalogZoneDescriptor("zone1", profiles)),
                List.of(new CatalogSchemaDescriptor(1, "desc", tables, indexes, views, 1))));

        SnapshotEntry deserialized = (SnapshotEntry) marshaller.unmarshall(marshaller.marshall(entry));

        BDDAssertions.assertThat(deserialized).usingRecursiveComparison().isEqualTo(entry);
    }

    private void renameIndexEntry() {
        var entry = new RenameIndexEntry(1, "newName");

        VersionedUpdate update = newVersionedUpdate(entry);

        assertVersionedUpdate(update, serialize(update));
    }

    private VersionedUpdate serialize(VersionedUpdate update) {
        byte[] bytes = marshaller.marshall(update);
        return (VersionedUpdate) marshaller.unmarshall(bytes);
    }

    private static void assertVersionedUpdate(VersionedUpdate expected, VersionedUpdate update) {
        assertThat(update.version(), is(expected.version()));
        assertThat(update.delayDurationMs(), is(expected.delayDurationMs()));

        int expectedSize = expected.entries().size();

        assertThat(update.entries(), hasSize(expectedSize));

        for (int i = 0; i < expectedSize; i++) {
            UpdateEntry expectedEntry = expected.entries().get(i);
            UpdateEntry actualEntry = update.entries().get(i);

            BDDAssertions.assertThat(actualEntry).usingRecursiveComparison().isEqualTo(expectedEntry);
        }
    }

    private static CatalogZoneDescriptor newCatalogZoneDescriptor(
            String zoneName, CatalogStorageProfilesDescriptor profiles) {
        int zoneId = 1;
        int partitions = 3;

        return new CatalogZoneDescriptor(
                zoneId,
                zoneName,
                partitions,
                3,
                1,
                2,
                3,
                DEFAULT_FILTER,
                profiles
        );
    }

    private static VersionedUpdate newVersionedUpdate(UpdateEntry ... entry) {
        int updateVer = 101;
        long delayDuration = Long.MIN_VALUE;

        return new VersionedUpdate(updateVer, delayDuration, List.of(entry));
    }

    private static CatalogTableColumnDescriptor newCatalogTableColumnDescriptor(String name, @Nullable DefaultValue defaultValue) {
        return new CatalogTableColumnDescriptor(name, ColumnType.STRING, false, 10, 5, 127, defaultValue);
    }

    private static CatalogSortedIndexDescriptor newSortedIndexDescriptor(String name) {
        CatalogIndexColumnDescriptor idxCol1 = new CatalogIndexColumnDescriptor("C1", CatalogColumnCollation.ASC_NULLS_FIRST);
        CatalogIndexColumnDescriptor idxCol2 = new CatalogIndexColumnDescriptor("C2", CatalogColumnCollation.DESC_NULLS_LAST);
        CatalogIndexColumnDescriptor idxCol3 = new CatalogIndexColumnDescriptor("C3", CatalogColumnCollation.DESC_NULLS_FIRST);
        CatalogIndexColumnDescriptor idxCol4 = new CatalogIndexColumnDescriptor("C4", CatalogColumnCollation.ASC_NULLS_LAST);

        return new CatalogSortedIndexDescriptor(
                1, name, 12, false, CatalogIndexStatus.AVAILABLE, 1, 0, List.of(idxCol1, idxCol2, idxCol3, idxCol4));
    }

    private static CatalogHashIndexDescriptor newHashIndexDescriptor(String name) {
        return new CatalogHashIndexDescriptor(
                1, name, 12, true, CatalogIndexStatus.REGISTERED, 1, 0, List.of("C1", "C2"));
    }

    private static CatalogTableDescriptor newTableDescriptor(String name, List<CatalogTableColumnDescriptor> columns) {
        return newTableDescriptor(name, columns, List.of(columns.get(0).name()), null);
    }

    private static CatalogTableDescriptor newTableDescriptor(
            String name,
            List<CatalogTableColumnDescriptor> columns,
            List<String> pkCols,
            @Nullable List<String> colCols
    ) {
        return new CatalogTableDescriptor(
                1,
                3,
                1,
                name,
                17,
                columns,
                pkCols,
                colCols,
                "default"
        );
    }

    private static class CustomDefaultValue implements Serializable {
        private static final long serialVersionUID = 0L;

        private final int field;

        CustomDefaultValue(int field) {
            this.field = field;
        }

        @Override
        public String toString() {
            return S.toString(this);
        }
    }
}
