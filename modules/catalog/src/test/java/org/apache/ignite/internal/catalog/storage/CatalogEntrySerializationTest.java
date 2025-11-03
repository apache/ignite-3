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
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;

import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import it.unimi.dsi.fastutil.shorts.ShortList;
import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
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
import org.apache.ignite.internal.catalog.descriptors.CatalogTableSchemaVersions;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableSchemaVersions.TableVersion;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogEntrySerializerProvider;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataInput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataOutput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntry;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataInput;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataOutput;
import org.apache.ignite.sql.ColumnType;
import org.assertj.core.api.BDDAssertions;
import org.assertj.core.api.RecursiveComparisonAssert;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.support.AnnotationConsumer;
import org.junit.jupiter.params.support.ParameterDeclarations;

/**
 * Tests to verify catalog storage entries serialization.
 *
 * <p>The main purpose of this class is to make sure that every marshallable entry can be marshalled and then
 * successfully unmarshalled back by the same version of serializer.
 */
public class CatalogEntrySerializationTest extends BaseIgniteAbstractTest {
    private static final int MOST_RECENT_VERSION = Integer.MAX_VALUE;

    private static final long SEED = System.nanoTime();

    private static final Random RND = new Random(SEED);

    /** This field should be ignored for version 1. */
    private static final String UPDATE_TIMESTAMP_FIELD_NAME_REGEX = ".*updateTimestamp";

    private AssertionConfiguration assertionConfiguration = assertion -> assertion;

    @BeforeEach
    public void setup() {
        log.info("Seed: {}", SEED);
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.ALTER_COLUMN)
    void alterColumnEntry(int serializerVersion) {
        CatalogTableColumnDescriptor desc1 = newCatalogTableColumnDescriptor("c0", null);
        CatalogTableColumnDescriptor desc2 =
                newCatalogTableColumnDescriptor("c1", DefaultValue.constant(UUID.randomUUID()));
        CatalogTableColumnDescriptor desc3 =
                newCatalogTableColumnDescriptor("c2", DefaultValue.functionCall("function"));
        CatalogTableColumnDescriptor desc4 = newCatalogTableColumnDescriptor("c3", DefaultValue.constant(null));

        checkSerialization(serializerVersion, new AlterColumnEntry(1, desc1));
        checkSerialization(serializerVersion, new AlterColumnEntry(1, desc2));
        checkSerialization(serializerVersion, new AlterColumnEntry(1, desc3));
        checkSerialization(serializerVersion, new AlterColumnEntry(1, desc4));
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.ALTER_ZONE)
    void alterZoneEntry(int serializerVersion) {
        CatalogStorageProfilesDescriptor profiles =
                new CatalogStorageProfilesDescriptor(List.of(new CatalogStorageProfileDescriptor("default")));
        UpdateEntry entry1 = new AlterZoneEntry(newCatalogZoneDescriptor("zone1", profiles));

        checkSerialization(serializerVersion, entry1);
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.NEW_ZONE)
    void newZoneEntry(int serializerVersion) {
        CatalogStorageProfilesDescriptor profiles =
                new CatalogStorageProfilesDescriptor(List.of(new CatalogStorageProfileDescriptor("default")));

        UpdateEntry entry1 = new NewZoneEntry(newCatalogZoneDescriptor("zone1", profiles));
        checkSerialization(serializerVersion, entry1);

        UpdateEntry entry2 = new NewZoneEntry(newCatalogZoneDescriptor("zone2", profiles));
        checkSerialization(serializerVersion, entry2);
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.DROP_COLUMN)
    void dropColumnEntry(int serializerVersion) {
        checkSerialization(serializerVersion, new DropColumnsEntry(1, Set.of("C1", "C2")));
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.DROP_INDEX)
    void dropIndexEntry(int serializerVersion) {
        checkSerialization(serializerVersion, new DropIndexEntry(231));
        checkSerialization(serializerVersion, new DropIndexEntry(465));
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.DROP_TABLE)
    void dropTableEntry(int serializerVersion) {
        checkSerialization(serializerVersion, new DropTableEntry(23));
        checkSerialization(serializerVersion, new DropTableEntry(3));
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.DROP_ZONE)
    void dropZoneEntry(int serializerVersion) {
        checkSerialization(serializerVersion, new DropZoneEntry(123));
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.MAKE_INDEX_AVAILABLE)
    void makeIndexAvailableEntry(int serializerVersion) {
        checkSerialization(serializerVersion, new MakeIndexAvailableEntry(321));
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.REMOVE_INDEX)
    void removeIndexEntry(int serializerVersion) {
        checkSerialization(serializerVersion, new RemoveIndexEntry(231));
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.START_BUILDING_INDEX)
    void startBuildingIndexEntry(int serializerVersion) {
        checkSerialization(serializerVersion, new StartBuildingIndexEntry(321));
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.NEW_COLUMN)
    void newColumnEntry(int serializerVersion) {
        CatalogTableColumnDescriptor columnDescriptor1 = newCatalogTableColumnDescriptor("c1", DefaultValue.constant(null));
        CatalogTableColumnDescriptor columnDescriptor2 = newCatalogTableColumnDescriptor("c2", DefaultValue.functionCall("func"));

        checkSerialization(serializerVersion, new NewColumnsEntry(11, List.of(columnDescriptor1, columnDescriptor2)));
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.NEW_INDEX)
    void newIndexEntry(int serializerVersion) {
        CatalogSortedIndexDescriptor sortedIndexDescriptor = newSortedIndexDescriptor(
                "idx1", serializerVersion == 1 ? 1 : MOST_RECENT_VERSION
        );
        CatalogHashIndexDescriptor hashIndexDescriptor = newHashIndexDescriptor(
                "idx2", serializerVersion == 1 ? 1 : MOST_RECENT_VERSION
        );

        checkSerialization(serializerVersion, new NewIndexEntry(sortedIndexDescriptor));
        checkSerialization(serializerVersion, new NewIndexEntry(hashIndexDescriptor));
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.NEW_SYS_VIEW)
    void newSystemViewEntry(int serializerVersion) {
        CatalogTableColumnDescriptor col1 = newCatalogTableColumnDescriptor("c1", null);
        CatalogTableColumnDescriptor col2 = newCatalogTableColumnDescriptor("c2", null);

        CatalogSystemViewDescriptor nodeDesc =
                new CatalogSystemViewDescriptor(1, 2, "view1", List.of(col1, col2), SystemViewType.NODE);
        CatalogSystemViewDescriptor clusterDesc =
                new CatalogSystemViewDescriptor(1, 2, "view1", List.of(col1, col2), SystemViewType.CLUSTER);

        NewSystemViewEntry nodeEntry = new NewSystemViewEntry(nodeDesc);
        NewSystemViewEntry clusterEntry = new NewSystemViewEntry(clusterDesc);

        checkSerialization(serializerVersion, nodeEntry);
        checkSerialization(serializerVersion, clusterEntry);
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.NEW_TABLE)
    void newTableEntry(int serializerVersion) {
        if (serializerVersion == 1) {
            ignoreDifferenceInUpdateTime();
        }

        CatalogTableColumnDescriptor col1 = newCatalogTableColumnDescriptor("c0", null);
        CatalogTableColumnDescriptor col2 = newCatalogTableColumnDescriptor("c1", null);
        CatalogTableColumnDescriptor col3 = newCatalogTableColumnDescriptor("c3", null);
        CatalogTableColumnDescriptor col4 = newCatalogTableColumnDescriptor("c2", null);

        List<CatalogTableColumnDescriptor> columns = List.of(col1, col2, col3, col4);

        checkSerialization(serializerVersion, new NewTableEntry(newTableDescriptor("Table1", columns, IntList.of(1, 2), null)));
        checkSerialization(serializerVersion, new NewTableEntry(newTableDescriptor("Table1", columns, IntList.of(1, 2), IntList.of())));
        checkSerialization(serializerVersion, new NewTableEntry(newTableDescriptor("Table1", columns, IntList.of(1, 2), IntList.of(2))));
        checkSerialization(serializerVersion, new NewTableEntry(newTableDescriptor("Table1", columns, IntList.of(1, 2), IntList.of(1))));
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.RENAME_TABLE)
    void renameTableEntry(int serializerVersion) {
        checkSerialization(serializerVersion, new RenameTableEntry(1, "newName"));
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.ID_GENERATOR)
    void idGeneratorEntry(int serializerVersion) {
        checkSerialization(serializerVersion, new ObjectIdGenUpdateEntry(Integer.MAX_VALUE));
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.SNAPSHOT)
    void snapshotEntry(int serializerVersion) {
        if (serializerVersion == 1) {
            ignoreDifferenceInUpdateTime();
        }

        CatalogTableColumnDescriptor col1 = newCatalogTableColumnDescriptor("c1", null);
        CatalogTableColumnDescriptor col2 = newCatalogTableColumnDescriptor("c2", null);

        List<CatalogTableColumnDescriptor> columns = List.of(col1, col2);

        CatalogTableDescriptor[] tables = {
                newTableDescriptor("Table1", columns),
                newTableDescriptor("Table2", columns)
        };

        CatalogIndexDescriptor[] indexes = {
                newSortedIndexDescriptor("idx1", serializerVersion == 1 ? 1 : MOST_RECENT_VERSION),
                newHashIndexDescriptor("idx2", serializerVersion == 1 ? 1 : MOST_RECENT_VERSION)
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
                List.of(new CatalogSchemaDescriptor(1, "desc", tables, indexes, views, hybridTimestamp(1))), zone1.id()));

        checkSerialization(serializerVersion, entry);
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.VERSIONED_UPDATE)
    void versionedUpdateEntry(int serializerVersion) {
        if (serializerVersion == 1) {
            ignoreDifferenceInUpdateTime();
        }

        CatalogTableColumnDescriptor col1 = newCatalogTableColumnDescriptor("c1", null);
        CatalogTableColumnDescriptor col2 = newCatalogTableColumnDescriptor("c2", null);

        List<CatalogTableColumnDescriptor> columns = List.of(col1, col2);

        CatalogStorageProfilesDescriptor profiles =
                new CatalogStorageProfilesDescriptor(List.of(new CatalogStorageProfileDescriptor("default")));

        VersionedUpdate entry = new VersionedUpdate(2, 321, List.of(
                new NewTableEntry(newTableDescriptor("Table1", columns)),
                new NewIndexEntry(newSortedIndexDescriptor("idx1", serializerVersion == 1 ? 1 : MOST_RECENT_VERSION)),
                new NewZoneEntry(newCatalogZoneDescriptor("zone1", profiles))
        ));

        checkSerialization(serializerVersion, entry);
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.RENAME_INDEX)
    void renameIndexEntry(int serializerVersion) {
        checkSerialization(serializerVersion, new RenameIndexEntry(1, "newName"));
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.SET_DEFAULT_ZONE)
    void setDefaultZoneEntry(int serializerVersion) {
        checkSerialization(serializerVersion, new SetDefaultZoneEntry(1));
        checkSerialization(serializerVersion, new SetDefaultZoneEntry(Integer.MAX_VALUE));
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.NEW_SCHEMA)
    void newSchemaEntry(int serializerVersion) {
        if (serializerVersion == 1) {
            ignoreDifferenceInUpdateTime();
        }

        checkSerialization(serializerVersion, new NewSchemaEntry(newSchemaDescriptor(
                serializerVersion == 1 ? 1 : MOST_RECENT_VERSION, "PUBLIC"
        )));
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.DROP_SCHEMA)
    void dropSchemaEntry(int serializerVersion) {
        checkSerialization(serializerVersion, new DropSchemaEntry(1));
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.DESCRIPTOR_HASH_INDEX)
    void descriptorHashIndex(int serializerVersion) {
        checkSerialization(serializerVersion, newHashIndexDescriptor("foo", serializerVersion));
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.DESCRIPTOR_SORTED_INDEX)
    void descriptorSortedIndex(int serializerVersion) {
        checkSerialization(serializerVersion, newSortedIndexDescriptor("foo", serializerVersion));
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.DESCRIPTOR_SCHEMA)
    void descriptorSchema(int serializerVersion) {
        if (serializerVersion == 1) {
            ignoreDifferenceInUpdateTime();
        }

        checkSerialization(serializerVersion, newSchemaDescriptor(serializerVersion, "my_schema1"));
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.DESCRIPTOR_STORAGE_PROFILE)
    void descriptorStorageProfile(int serializerVersion) {
        checkSerialization(serializerVersion, new CatalogStorageProfileDescriptor("profile1"));
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.DESCRIPTOR_STORAGE_PROFILES)
    void descriptorStorageProfiles(int serializerVersion) {
        checkSerialization(serializerVersion, new CatalogStorageProfilesDescriptor(List.of(
                new CatalogStorageProfileDescriptor("profile1"),
                new CatalogStorageProfileDescriptor("profile2")
        )));
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.DESCRIPTOR_SYSTEM_VIEW)
    void descriptorSystemView(int serializerVersion) {
        CatalogTableColumnDescriptor column = newCatalogTableColumnDescriptor("column", null);
        CatalogSystemViewDescriptor view = new CatalogSystemViewDescriptor(1, 2, "sys_view", List.of(column), SystemViewType.NODE);
        checkSerialization(serializerVersion, view);
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.DESCRIPTOR_TABLE)
    void descriptorTable(int serializerVersion) {
        if (serializerVersion == 1) {
            ignoreDifferenceInUpdateTime();
        }

        checkSerialization(serializerVersion, newTableDescriptor("some_table", List.of(newCatalogTableColumnDescriptor("c1", null))));
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.DESCRIPTOR_TABLE_COLUMN)
    void descriptorTableColumn(int serializerVersion) {
        checkSerialization(serializerVersion, newCatalogTableColumnDescriptor("c1", null));
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.DESCRIPTOR_TABLE_VERSION)
    void descriptorTableVersion(int serializerVersion) {
        checkSerialization(serializerVersion, new TableVersion(List.of(newCatalogTableColumnDescriptor("column", null))));
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.DESCRIPTOR_TABLE_SCHEMA_VERSIONS)
    void descriptorTableSchemaVersions(int serializerVersion) {
        CatalogTableSchemaVersions versions = new CatalogTableSchemaVersions(
                new TableVersion(List.of(newCatalogTableColumnDescriptor("column1", null)))
        );

        versions = versions.append(new TableVersion(List.of(
                newCatalogTableColumnDescriptor("column1", null),
                newCatalogTableColumnDescriptor("column2", null)
        )));

        checkSerialization(serializerVersion, versions);
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.DESCRIPTOR_ZONE)
    void descriptorZone(int serializerVersion) {
        CatalogStorageProfilesDescriptor profiles =
                new CatalogStorageProfilesDescriptor(List.of(new CatalogStorageProfileDescriptor("default")));
        checkSerialization(serializerVersion, newCatalogZoneDescriptor("myZone", profiles));
    }

    @ParameterizedTest(name = "serializerVersion={0}")
    @MarshallableEntryTypeSource(MarshallableEntryType.ALTER_TABLE_PROPERTIES)
    void alterTablePropertiesEntry(int serializerVersion) {
        checkSerialization(serializerVersion, new AlterTablePropertiesEntry(123, null, null));
        checkSerialization(serializerVersion, new AlterTablePropertiesEntry(123, 0.2, null));
        checkSerialization(serializerVersion, new AlterTablePropertiesEntry(123, null, 500L));
        checkSerialization(serializerVersion, new AlterTablePropertiesEntry(123, 0.2, 500L));
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

        // TODO Include ignored values to test after https://issues.apache.org/jira/browse/IGNITE-17373
        //  list.add(Duration.of(11, ChronoUnit.HOURS));
        //  list.add(Period.of(5, 4, 3));

        return list.stream().map(val -> {
            NativeType nativeType = NativeTypes.fromObject(val);
            return Arguments.of(nativeType == null ? ColumnType.NULL : nativeType.spec(), val);
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

    @Test
    void ensureAllMarshallableEntryTypesAreCovered() {
        EnumSet<MarshallableEntryType> missedTypes = EnumSet.allOf(MarshallableEntryType.class);

        for (Method method : CatalogEntrySerializationTest.class.getDeclaredMethods()) {
            if (!method.isAnnotationPresent(MarshallableEntryTypeSource.class)) {
                continue;
            }

            MarshallableEntryTypeSource typeSource = method.getAnnotation(MarshallableEntryTypeSource.class);

            missedTypes.remove(typeSource.value());
        }

        assertThat("All marshallable entries must be covered. " 
                + "Please add new test method for every missed entry type using following template:\n\n"
                + "    @ParameterizedTest(name = \"serializerVersion={0}\")\n"
                + "    @MarshallableEntryTypeSource(MarshallableEntryType.MY_ENTRY_TYPE)\n"
                + "    void myMarshallableEntry(int serializerVersion) {\n"
                + "        MarshallableEntry entry = ... // create entry here\n"
                + "\n"
                + "        checkSerialization(serializerVersion, entry);\n"
                + "    }\n", missedTypes, empty());
    }

    private void checkSerialization(int serializerVersion, MarshallableEntry entry) {
        CatalogEntrySerializerProvider serializers = CatalogEntrySerializerProvider.DEFAULT_PROVIDER;

        try {
            byte[] bytes;

            try (CatalogObjectDataOutput output = new CatalogObjectDataOutput(serializers)) {
                serializers.get(serializerVersion, entry.typeId()).writeTo(entry, output);

                bytes = output.array();
            }

            try (CatalogObjectDataInput input = new CatalogObjectDataInput(serializers, bytes)) {
                MarshallableEntry deserialized = serializers.get(serializerVersion, entry.typeId()).readFrom(input);

                assertEqualsRecursive(entry, deserialized);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
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
                2, // default
                2,
                3,
                DEFAULT_FILTER,
                profiles,
                ConsistencyMode.STRONG_CONSISTENCY
        );
    }

    private static CatalogTableColumnDescriptor newCatalogTableColumnDescriptor(String name, @Nullable DefaultValue defaultValue) {
        return new CatalogTableColumnDescriptor(name, ColumnType.STRING, false, 10, 5, 127, defaultValue);
    }

    @SuppressWarnings("removal")
    private static CatalogSortedIndexDescriptor newSortedIndexDescriptor(String name, int indexSerializerVersion) {
        List<CatalogIndexColumnDescriptor> columns;
        switch (indexSerializerVersion) {
            case 1:
            case 2: {
                CatalogIndexColumnDescriptor idxCol1 = new CatalogIndexColumnDescriptor("c1", CatalogColumnCollation.ASC_NULLS_FIRST);
                CatalogIndexColumnDescriptor idxCol2 = new CatalogIndexColumnDescriptor("c2", CatalogColumnCollation.DESC_NULLS_LAST);
                CatalogIndexColumnDescriptor idxCol3 = new CatalogIndexColumnDescriptor("c3", CatalogColumnCollation.DESC_NULLS_FIRST);
                CatalogIndexColumnDescriptor idxCol4 = new CatalogIndexColumnDescriptor("c4", CatalogColumnCollation.ASC_NULLS_LAST);

                columns = List.of(idxCol1, idxCol2, idxCol3, idxCol4);

                break;
            }
            default: // the most recent version
                CatalogIndexColumnDescriptor idxCol1 = new CatalogIndexColumnDescriptor(1, CatalogColumnCollation.ASC_NULLS_FIRST);
                CatalogIndexColumnDescriptor idxCol2 = new CatalogIndexColumnDescriptor(3, CatalogColumnCollation.DESC_NULLS_LAST);
                CatalogIndexColumnDescriptor idxCol3 = new CatalogIndexColumnDescriptor(5, CatalogColumnCollation.DESC_NULLS_FIRST);
                CatalogIndexColumnDescriptor idxCol4 = new CatalogIndexColumnDescriptor(7, CatalogColumnCollation.ASC_NULLS_LAST);

                columns = List.of(idxCol1, idxCol2, idxCol3, idxCol4);
        }

        return new CatalogSortedIndexDescriptor(
                1, name, 12, false, CatalogIndexStatus.AVAILABLE, columns, true
        );
    }

    private static CatalogHashIndexDescriptor newHashIndexDescriptor(String name, int indexSerializerVersion) {
        switch (indexSerializerVersion) {
            case 1:
            case 2:
                //noinspection removal
                return new CatalogHashIndexDescriptor(
                        1, name, 12, true, CatalogIndexStatus.REGISTERED, List.of("c1", "c2"), true
                );
            default: // the most recent version
                return new CatalogHashIndexDescriptor(
                        1, name, 12, true, CatalogIndexStatus.REGISTERED, IntList.of(1, 2), true
                );
        }
    }

    private static CatalogTableDescriptor newTableDescriptor(String name, List<CatalogTableColumnDescriptor> columns) {
        return newTableDescriptor(name, columns, IntList.of(0), null);
    }

    private static CatalogTableDescriptor newTableDescriptor(
            String name,
            List<CatalogTableColumnDescriptor> columns,
            IntList pkCols,
            @Nullable IntList colCols
    ) {
        return CatalogTableDescriptor.builder()
                .id(1)
                .schemaId(3)
                .primaryKeyIndexId(1)
                .name(name)
                .zoneId(17)
                .newColumns(columns)
                .primaryKeyColumns(pkCols)
                .colocationColumns(colCols)
                .storageProfile("default")
                .timestamp(hybridTimestamp(3))
                .build();
    }

    private static CatalogSchemaDescriptor newSchemaDescriptor(int schemaSerializerVersion, String name) {
        CatalogIndexDescriptor[] indexes = {
                newSortedIndexDescriptor("idx11", schemaSerializerVersion == 1 ? 1 : MOST_RECENT_VERSION),
                newHashIndexDescriptor("idx21", schemaSerializerVersion == 1 ? 1 : MOST_RECENT_VERSION)
        };

        CatalogTableColumnDescriptor col1 = newCatalogTableColumnDescriptor("c1", null);
        CatalogTableColumnDescriptor col2 = newCatalogTableColumnDescriptor("c2", null);

        List<CatalogTableColumnDescriptor> columns = List.of(col1, col2);

        CatalogTableDescriptor[] tables = {
                newTableDescriptor("Table1", columns),
                newTableDescriptor("Table2", columns)
        };

        CatalogSystemViewDescriptor[] views = {
                new CatalogSystemViewDescriptor(1, 2, "view1", columns, SystemViewType.NODE),
                new CatalogSystemViewDescriptor(1, 2, "view2", columns, SystemViewType.CLUSTER)
        };

        return new CatalogSchemaDescriptor(1, name, tables, indexes, views, hybridTimestamp(3));
    }

    private <T> void assertEqualsRecursive(T expected, T actual) {
        RecursiveComparisonAssert<?> assertion = assertionConfiguration.apply(
                BDDAssertions.assertThat(actual).usingRecursiveComparison()
        );

        assertion.isEqualTo(expected);
    }

    private void ignoreDifferenceInUpdateTime() {
        assertionConfiguration = assertionConfiguration.andThen(assertion ->
                assertion.ignoringFieldsMatchingRegexes(UPDATE_TIMESTAMP_FIELD_NAME_REGEX));
    }

    static class MarshallableEntryTypeArgumentsProvider
            implements ArgumentsProvider, AnnotationConsumer<MarshallableEntryTypeSource> {

        private MarshallableEntryType enumValue;

        @Override
        public void accept(MarshallableEntryTypeSource annotation) {
            this.enumValue = annotation.value();
        }

        @Override
        public Stream<? extends Arguments> provideArguments(ParameterDeclarations parameters, ExtensionContext context) {
            return resolveVersions(enumValue.container()).intStream().mapToObj(Arguments::of);
        }

        private static ShortList resolveVersions(Class<?> clazz) {
            ShortList versions = new ShortArrayList();

            for (Class<?> declaredClass : clazz.getDeclaredClasses()) {
                if (CatalogObjectSerializer.class.isAssignableFrom(declaredClass)) {
                    CatalogSerializer catalogSerializer = declaredClass.getAnnotation(CatalogSerializer.class);

                    versions.add(catalogSerializer.version());
                }
            }

            assertThat(versions, not(empty()));

            return versions;
        }
    }

    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @ArgumentsSource(MarshallableEntryTypeArgumentsProvider.class)
    @interface MarshallableEntryTypeSource {
        MarshallableEntryType value();
    }

    @FunctionalInterface
    private interface AssertionConfiguration extends Function<RecursiveComparisonAssert<?>, RecursiveComparisonAssert<?>> {
        default AssertionConfiguration andThen(AssertionConfiguration after) {
            return assertion -> after.apply(apply(assertion));
        }
    }
}
