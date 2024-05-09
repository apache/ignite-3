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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.DefaultValue.ConstantValue;
import org.apache.ignite.internal.catalog.commands.DefaultValue.FunctionCall;
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
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataInput;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataOutput;
import org.apache.ignite.sql.ColumnType;
import org.assertj.core.api.BDDAssertions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify catalog storage entries serialization.
 */
public class CatalogEntrySerializationTest extends BaseIgniteAbstractTest {
    private static final long SEED = System.nanoTime();

    private static final Random RND = new Random(SEED);

    private final UpdateLogMarshallerImpl marshaller = new UpdateLogMarshallerImpl();

    @BeforeEach
    public void setup() {
        log.info("Seed: {}", SEED);
    }

    @ParameterizedTest
    @EnumSource(value = MarshallableEntryType.class, names = "VERSIONED_UPDATE", mode = Mode.EXCLUDE)
    void test(MarshallableEntryType type) {
        switch (type) {
            case ALTER_COLUMN:
                checkAlterColumnEntry();
                break;

            case ALTER_ZONE:
                checkAlterZoneEntry();
                break;

            case NEW_ZONE:
                checkNewZoneEntry();
                break;

            case DROP_COLUMN:
                checkSerialization(new DropColumnsEntry(1, Set.of("C1", "C2")));
                break;

            case DROP_INDEX:
                checkSerialization(new DropIndexEntry(231, 23), new DropIndexEntry(231, 1));
                break;

            case DROP_TABLE:
                checkSerialization(new DropTableEntry(23), new DropTableEntry(3));
                break;

            case DROP_ZONE:
                checkSerialization(new DropZoneEntry(123));
                break;

            case MAKE_INDEX_AVAILABLE:
                checkSerialization(new MakeIndexAvailableEntry(321));
                break;

            case REMOVE_INDEX:
                checkSerialization(new RemoveIndexEntry(231));
                break;

            case START_BUILDING_INDEX:
                checkSerialization(new StartBuildingIndexEntry(321));
                break;

            case NEW_COLUMN:
                checkNewColumnsEntry();
                break;

            case NEW_INDEX:
                checkNewIndexEntry();
                break;

            case NEW_SYS_VIEW:
                checkNewSystemViewEntry();
                break;

            case NEW_TABLE:
                checkNewTableEntry();
                break;

            case RENAME_TABLE:
                checkSerialization(new RenameTableEntry(1, "newName"));
                break;

            case ID_GENERATOR:
                checkSerialization(new ObjectIdGenUpdateEntry(Integer.MAX_VALUE));
                break;

            case SNAPSHOT:
                checkSnapshotEntry();
                break;

            case RENAME_INDEX:
                checkSerialization(new RenameIndexEntry(1, "newName"));
                break;

            case SET_DEFAULT_ZONE:
                checkSerialization(new SetDefaultZoneEntry(1), new SetDefaultZoneEntry(Integer.MAX_VALUE));
                break;

            case NEW_SCHEMA:
                checkSerialization(new NewSchemaEntry(new CatalogSchemaDescriptor(
                        0, "S", new CatalogTableDescriptor[0], new CatalogIndexDescriptor[0], new CatalogSystemViewDescriptor[0], 0)));
                break;

            default:
                throw new UnsupportedOperationException("Test not implemented " + type);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("values")
    public void testConstantDefaultAllTypes(ColumnType columnType, Object value) throws IOException {
        ConstantValue val = (ConstantValue) DefaultValue.constant(value);

        log.info("{}: {}", columnType, value);

        try (IgniteUnsafeDataOutput os = new IgniteUnsafeDataOutput(128)) {
            DefaultValue.writeTo(val, os);

            try (IgniteUnsafeDataInput in = new IgniteUnsafeDataInput(os.internalArray())) {
                DefaultValue actual = DefaultValue.readFrom(in);
                assertEquals(val, actual);
            }
        }
    }

    private static Stream<Arguments> values() {
        List<Object> list = new ArrayList<>();

        list.add(null);
        list.add(RND.nextBoolean());

        list.add((byte) RND.nextInt());
        list.add((short) RND.nextInt());
        list.add(RND.nextInt());
        list.add(RND.nextLong());
        list.add((float) RND.nextDouble());
        list.add(RND.nextDouble());

        list.add(BigDecimal.valueOf(RND.nextLong()));
        list.add(BigDecimal.valueOf(RND.nextLong(), RND.nextInt(100)));

        list.add(BigInteger.valueOf(RND.nextLong()));

        list.add(LocalTime.of(RND.nextInt(24), RND.nextInt(60), RND.nextInt(60), RND.nextInt(100_000)));
        list.add(LocalDate.of(RND.nextInt(4000) - 1000, RND.nextInt(12) + 1, RND.nextInt(27) + 1));
        list.add(LocalDateTime.of(
                LocalDate.of(RND.nextInt(4000) - 1000, RND.nextInt(12) + 1, RND.nextInt(27) + 1),
                LocalTime.of(RND.nextInt(24), RND.nextInt(60), RND.nextInt(60), RND.nextInt(100_000))
        ));

        byte[] bytes = new byte[RND.nextInt(1000)];
        RND.nextBytes(bytes);
        list.add(Base64.getEncoder().encodeToString(bytes));

        list.add(UUID.randomUUID());

        // TODO Include ignored values to test after https://issues.apache.org/jira/browse/IGNITE-15200
        //  list.add(Duration.of(11, ChronoUnit.HOURS));
        //  list.add(Period.of(5, 4, 3));

        BitSet bitSet = new BitSet();
        for (int i = 0; i < RND.nextInt(100); i++) {
            int b = RND.nextInt(1024);
            bitSet.set(b);
        }
        list.add(bitSet);

        return list.stream().map(val -> {
            NativeType nativeType = NativeTypes.fromObject(val);
            return Arguments.of(nativeType == null ? ColumnType.NULL : nativeType.spec().asColumnType(), val);
        });
    }

    @Test
    public void testFunctionCallDefault() throws IOException {
        FunctionCall val = (FunctionCall) DefaultValue.functionCall("func");

        try (IgniteUnsafeDataOutput os = new IgniteUnsafeDataOutput(128)) {
            DefaultValue.writeTo(val, os);

            try (IgniteUnsafeDataInput in = new IgniteUnsafeDataInput(os.internalArray())) {
                DefaultValue actual = DefaultValue.readFrom(in);
                assertEquals(val, actual);
            }
        }
    }

    private void checkAlterZoneEntry() {
        CatalogStorageProfilesDescriptor profiles =
                new CatalogStorageProfilesDescriptor(List.of(new CatalogStorageProfileDescriptor("default")));
        UpdateEntry entry1 = new AlterZoneEntry(newCatalogZoneDescriptor("zone1", profiles));

        VersionedUpdate update = newVersionedUpdate(entry1, entry1);

        assertVersionedUpdate(update, serialize(update));
    }

    private void checkNewZoneEntry() {
        CatalogStorageProfilesDescriptor profiles =
                new CatalogStorageProfilesDescriptor(List.of(new CatalogStorageProfileDescriptor("default")));

        UpdateEntry entry1 = new NewZoneEntry(newCatalogZoneDescriptor("zone1", profiles));
        UpdateEntry entry2 = new NewZoneEntry(newCatalogZoneDescriptor("zone2", profiles));
        VersionedUpdate update = newVersionedUpdate(entry1, entry2);

        assertVersionedUpdate(update, serialize(update));
    }

    private void checkAlterColumnEntry() {
        CatalogTableColumnDescriptor desc1 = newCatalogTableColumnDescriptor("c0", null);
        CatalogTableColumnDescriptor desc2 =
                newCatalogTableColumnDescriptor("c1", DefaultValue.constant(UUID.randomUUID()));
        CatalogTableColumnDescriptor desc3 =
                newCatalogTableColumnDescriptor("c2", DefaultValue.functionCall("function"));
        CatalogTableColumnDescriptor desc4 = newCatalogTableColumnDescriptor("c3", DefaultValue.constant(null));

        UpdateEntry entry1 = new AlterColumnEntry(1, desc1);
        UpdateEntry entry2 = new AlterColumnEntry(1, desc2);
        UpdateEntry entry3 = new AlterColumnEntry(1, desc3);
        UpdateEntry entry4 = new AlterColumnEntry(1, desc4);

        VersionedUpdate update = newVersionedUpdate(entry1, entry2, entry3, entry4);

        assertVersionedUpdate(update, serialize(update));
    }

    private void checkNewColumnsEntry() {
        CatalogTableColumnDescriptor columnDescriptor1 = newCatalogTableColumnDescriptor("c1", DefaultValue.constant(null));
        CatalogTableColumnDescriptor columnDescriptor2 = newCatalogTableColumnDescriptor("c2", DefaultValue.functionCall("func"));

        NewColumnsEntry entry = new NewColumnsEntry(11, List.of(columnDescriptor1, columnDescriptor2));

        VersionedUpdate update = newVersionedUpdate(entry);

        assertVersionedUpdate(update, serialize(update));
    }

    private void checkNewIndexEntry() {
        CatalogSortedIndexDescriptor sortedIndexDescriptor = newSortedIndexDescriptor("idx1");
        CatalogHashIndexDescriptor hashIndexDescriptor = newHashIndexDescriptor("idx2");

        NewIndexEntry sortedIdxEntry = new NewIndexEntry(sortedIndexDescriptor, 1);
        NewIndexEntry hashIdxEntry = new NewIndexEntry(hashIndexDescriptor, 1);

        VersionedUpdate update = newVersionedUpdate(sortedIdxEntry, hashIdxEntry);

        assertVersionedUpdate(update, serialize(update));
    }

    private void checkNewTableEntry() {
        CatalogTableColumnDescriptor col1 = newCatalogTableColumnDescriptor("c0", null);
        CatalogTableColumnDescriptor col2 = newCatalogTableColumnDescriptor("c1", null);
        CatalogTableColumnDescriptor col3 = newCatalogTableColumnDescriptor("c3", null);
        CatalogTableColumnDescriptor col4 = newCatalogTableColumnDescriptor("c2", null);

        List<CatalogTableColumnDescriptor> columns = List.of(col1, col2, col3, col4);

        NewTableEntry entry1 = new NewTableEntry(newTableDescriptor("Table1", columns, List.of("c1", "c2"), null));
        NewTableEntry entry2 = new NewTableEntry(newTableDescriptor("Table1", columns, List.of("c1", "c2"), List.of()));
        NewTableEntry entry3 = new NewTableEntry(newTableDescriptor("Table1", columns, List.of("c1", "c2"), List.of("c2")));
        NewTableEntry entry4 = new NewTableEntry(newTableDescriptor("Table1", columns, List.of("c1", "c2"), List.of("c1")));

        VersionedUpdate update = newVersionedUpdate(entry1, entry2, entry3, entry4);
        VersionedUpdate deserialized = serialize(update);

        assertVersionedUpdate(update, deserialized);

        NewTableEntry deserializedEntry = (NewTableEntry) deserialized.entries().get(0);
        assertSame(deserializedEntry.descriptor().primaryKeyColumns(), deserializedEntry.descriptor().colocationColumns());
    }

    private void checkNewSystemViewEntry() {
        CatalogTableColumnDescriptor col1 = newCatalogTableColumnDescriptor("c1", null);
        CatalogTableColumnDescriptor col2 = newCatalogTableColumnDescriptor("c2", null);

        CatalogSystemViewDescriptor nodeDesc =
                new CatalogSystemViewDescriptor(1, 2, "view1", List.of(col1, col2), SystemViewType.NODE);
        CatalogSystemViewDescriptor clusterDesc =
                new CatalogSystemViewDescriptor(1, 2, "view1", List.of(col1, col2), SystemViewType.CLUSTER);

        NewSystemViewEntry nodeEntry = new NewSystemViewEntry(nodeDesc);
        NewSystemViewEntry clusterEntry = new NewSystemViewEntry(clusterDesc);

        VersionedUpdate update = newVersionedUpdate(nodeEntry, clusterEntry);

        assertVersionedUpdate(update, serialize(update));
    }

    private void checkSnapshotEntry() {
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
                new CatalogSystemViewDescriptor(1, 2, "view1", columns, SystemViewType.NODE),
                new CatalogSystemViewDescriptor(1, 2, "view2", columns, SystemViewType.CLUSTER)
        };

        CatalogStorageProfilesDescriptor profiles =
                new CatalogStorageProfilesDescriptor(List.of(new CatalogStorageProfileDescriptor("default")));

        CatalogZoneDescriptor zone1 = newCatalogZoneDescriptor("zone1", profiles);

        SnapshotEntry entry = new SnapshotEntry(new Catalog(2, 0L, 1,
                List.of(zone1),
                List.of(new CatalogSchemaDescriptor(1, "desc", tables, indexes, views, 1)), zone1.id()));

        SnapshotEntry deserialized = (SnapshotEntry) marshaller.unmarshall(marshaller.marshall(entry));

        BDDAssertions.assertThat(deserialized).usingRecursiveComparison().isEqualTo(entry);
    }

    private VersionedUpdate serialize(VersionedUpdate update) {
        byte[] bytes = marshaller.marshall(update);
        return (VersionedUpdate) marshaller.unmarshall(bytes);
    }

    private void checkSerialization(UpdateEntry ... entry) {
        VersionedUpdate update = newVersionedUpdate(entry);

        assertVersionedUpdate(update, serialize(update));
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
                1, name, 12, false, CatalogIndexStatus.AVAILABLE, 1, List.of(idxCol1, idxCol2, idxCol3, idxCol4));
    }

    private static CatalogHashIndexDescriptor newHashIndexDescriptor(String name) {
        return new CatalogHashIndexDescriptor(
                1, name, 12, true, CatalogIndexStatus.REGISTERED, 1, List.of("C1", "C2"));
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
}
