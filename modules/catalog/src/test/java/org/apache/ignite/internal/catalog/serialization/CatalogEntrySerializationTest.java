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

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.catalog.descriptors.CatalogDataStorageDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor.SystemViewType;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableSchemaVersions;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableSchemaVersions.TableVersion;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.AlterColumnEntry;
import org.apache.ignite.internal.catalog.storage.AlterZoneEntry;
import org.apache.ignite.internal.catalog.storage.DropColumnsEntry;
import org.apache.ignite.internal.catalog.storage.DropIndexEntry;
import org.apache.ignite.internal.catalog.storage.DropTableEntry;
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
import org.apache.ignite.internal.catalog.storage.VersionedUpdate;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataInput;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataOutput;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Tests to verify catalog storage entries serialization.
 */
public class CatalogEntrySerializationTest extends BaseIgniteAbstractTest {
    private final UpdateLogMarshallerImpl marshaller = new UpdateLogMarshallerImpl();

    @Test
    public void alterZoneEntrySerialization() {
        UpdateEntry entry = new AlterZoneEntry(newCatalogZoneDescriptor());
        VersionedUpdate update = newVersionedUpdate(entry, entry);

        assertVersionedUpdate(update, serialize(update));
    }

    @Test
    public void newZoneEntrySerialization() {
        UpdateEntry entry = new NewZoneEntry(newCatalogZoneDescriptor());
        VersionedUpdate update = newVersionedUpdate(entry, entry);

        assertVersionedUpdate(update, serialize(update));
    }

    @Test
    public void alterColumnEntrySerialization() {
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

    @Test
    public void dropColumnsEntrySerialization() {
        DropColumnsEntry entry = new DropColumnsEntry(1, Set.of("C1", "C2"), "PUBLIC");

        VersionedUpdate update = newVersionedUpdate(entry);

        assertVersionedUpdate(update, serialize(update));
    }

    @Test
    public void dropIndexEntrySerialization() {
        DropIndexEntry entry = new DropIndexEntry(231, 23, "PUBLIC");

        VersionedUpdate update = newVersionedUpdate(entry);

        assertVersionedUpdate(update, serialize(update));
    }

    @Test
    public void dropTableEntrySerialization() {
        DropTableEntry entry = new DropTableEntry(23, "PUBLIC");

        VersionedUpdate update = newVersionedUpdate(entry);

        assertVersionedUpdate(update, serialize(update));
    }

    @Test
    public void makeIndexAvailableEntrySerialization() {
        MakeIndexAvailableEntry entry = new MakeIndexAvailableEntry(321);

        VersionedUpdate update = newVersionedUpdate(entry);

        assertVersionedUpdate(update, serialize(update));
    }

    @Test
    public void startBuildingIndexEntrySerialization() {
        StartBuildingIndexEntry entry = new StartBuildingIndexEntry(321);

        VersionedUpdate update = newVersionedUpdate(entry);

        assertVersionedUpdate(update, serialize(update));
    }

    @Test
    public void newColumnsEntrySerialization() {
        CatalogTableColumnDescriptor columnDescriptor1 = newCatalogTableColumnDescriptor("c1", DefaultValue.constant(null));
        CatalogTableColumnDescriptor columnDescriptor2 = newCatalogTableColumnDescriptor("c2", DefaultValue.functionCall("func"));

        NewColumnsEntry entry = new NewColumnsEntry(11, List.of(columnDescriptor1, columnDescriptor2), "PUBLIC");

        VersionedUpdate update = newVersionedUpdate(entry);

        assertVersionedUpdate(update, serialize(update));
    }

    @Test
    public void newIndexEntrySerialization() {
        CatalogSortedIndexDescriptor sortedIndexDescriptor = newSortedIndexDescriptor("idx1");
        CatalogHashIndexDescriptor hashIndexDescriptor = newHashIndexDescriptor("idx2");

        NewIndexEntry sortedIdxEntry = new NewIndexEntry(sortedIndexDescriptor, "PUBLIC");
        NewIndexEntry hashIdxEntry = new NewIndexEntry(hashIndexDescriptor, "PUBLIC");

        VersionedUpdate update = newVersionedUpdate(sortedIdxEntry, hashIdxEntry);

        assertVersionedUpdate(update, serialize(update));
    }

    @Test
    public void newTableEntrySerialization() {
        CatalogTableColumnDescriptor col1 = newCatalogTableColumnDescriptor("c1", null);
        CatalogTableColumnDescriptor col2 = newCatalogTableColumnDescriptor("c2", null);

        List<CatalogTableColumnDescriptor> columns = List.of(col1, col2);

        NewTableEntry entry = new NewTableEntry(newTableDescriptor("Table1", columns), "PUBLIC");

        VersionedUpdate update = newVersionedUpdate(entry, entry);

        assertVersionedUpdate(update, serialize(update));
    }

    @Test
    public void schemaDescriptorSerialization() throws IOException {
        CatalogTableColumnDescriptor col1 = newCatalogTableColumnDescriptor("c1", null);
        CatalogTableColumnDescriptor col2 = newCatalogTableColumnDescriptor("c2", null);

        List<CatalogTableColumnDescriptor> columns = Arrays.asList(col1, col2);

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

        CatalogEntrySerializer<CatalogSchemaDescriptor> serializer = CatalogSchemaDescriptor.SERIALIZER;

        CatalogSchemaDescriptor descriptor = new CatalogSchemaDescriptor(1, "desc", tables, indexes, views, 1);

        IgniteUnsafeDataOutput output = new IgniteUnsafeDataOutput(32);

        serializer.writeTo(descriptor, 1, output);

        byte[] bytes = output.array();

        IgniteUnsafeDataInput input = new IgniteUnsafeDataInput(bytes);

        CatalogSchemaDescriptor deserialized = serializer.readFrom(1, input);

        assertThat(deserialized.toString(), equalTo(descriptor.toString()));
    }

    @Test
    public void newSystemViewEntrySerialization() {
        CatalogTableColumnDescriptor col1 = newCatalogTableColumnDescriptor("c1", null);
        CatalogTableColumnDescriptor col2 = newCatalogTableColumnDescriptor("c2", null);

        CatalogSystemViewDescriptor nodeDesc =
                new CatalogSystemViewDescriptor(1, "view1", Arrays.asList(col1, col2), SystemViewType.NODE);
        CatalogSystemViewDescriptor clusterDesc =
                new CatalogSystemViewDescriptor(1, "view1", Arrays.asList(col1, col2), SystemViewType.CLUSTER);

        NewSystemViewEntry nodeEntry = new NewSystemViewEntry(nodeDesc, "PUBLIC");
        NewSystemViewEntry clusterEntry = new NewSystemViewEntry(clusterDesc, "PUBLIC");

        VersionedUpdate update = newVersionedUpdate(nodeEntry, clusterEntry);

        assertVersionedUpdate(update, serialize(update));
    }

    @Test
    public void renameTableEntrySerialization() {
        RenameTableEntry entry = new RenameTableEntry(1, "newName");

        VersionedUpdate update = newVersionedUpdate(entry);

        assertVersionedUpdate(update, serialize(update));
    }

    @Test
    public void objectIdGenUpdateEntrySerialization() {
        ObjectIdGenUpdateEntry entry = new ObjectIdGenUpdateEntry(Integer.MAX_VALUE);

        VersionedUpdate update = newVersionedUpdate(entry);

        assertVersionedUpdate(update, serialize(update));
    }

    private VersionedUpdate serialize(VersionedUpdate update) {
        byte[] bytes = marshaller.marshall(update);
        return marshaller.unmarshall(bytes);
    }

    private static void assertVersionedUpdate(VersionedUpdate expected, VersionedUpdate update) {
        assertThat(update.version(), is(expected.version()));
        assertThat(update.delayDurationMs(), is(expected.delayDurationMs()));

        int expectedSize = expected.entries().size();

        assertThat(update.entries(), hasSize(expectedSize));

        for (int i = 0; i < expectedSize; i++) {
            UpdateEntry expectedEntry = expected.entries().get(i);
            UpdateEntry actualEntry = update.entries().get(i);

            // TODO think.
            assertThat(actualEntry.toString(), equalTo(expectedEntry.toString()));
        }
    }

    private static CatalogZoneDescriptor newCatalogZoneDescriptor() {
        int zoneId = 1;
        int partitions = 3;

        CatalogDataStorageDescriptor storage = new CatalogDataStorageDescriptor("test-engine", null);

        return new CatalogZoneDescriptor(
                zoneId,
                "zone",
                partitions,
                3,
                1,
                2,
                3,
                DEFAULT_FILTER,
                storage
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
        CatalogIndexColumnDescriptor colDesc1 = new CatalogIndexColumnDescriptor("C1", CatalogColumnCollation.ASC_NULLS_FIRST);
        CatalogIndexColumnDescriptor colDesc2 = new CatalogIndexColumnDescriptor("C2", CatalogColumnCollation.DESC_NULLS_LAST);

        return new CatalogSortedIndexDescriptor(1, name, 12, false, Arrays.asList(colDesc1, colDesc2), CatalogIndexStatus.AVAILABLE);
    }

    private static CatalogHashIndexDescriptor newHashIndexDescriptor(String name) {
        return new CatalogHashIndexDescriptor(1, "idx2", 12, true, Arrays.asList("C1", "C2"), CatalogIndexStatus.REGISTERED);
    }

    private static CatalogTableDescriptor newTableDescriptor(String name, List<CatalogTableColumnDescriptor> columns) {
        return new CatalogTableDescriptor(
                1,
                3,
                1,
                name,
                17,
                columns,
                Arrays.asList(columns.get(0).name()),
                Arrays.asList(columns.get(0).name()),
                new CatalogTableSchemaVersions(321, new TableVersion(columns)),
                123,
                121
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
