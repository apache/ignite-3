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

package org.apache.ignite.internal.sql.engine.schema;

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.columnType2NativeType;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogTestUtils;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexCommand;
import org.apache.ignite.internal.catalog.commands.CreateSortedIndexCommand;
import org.apache.ignite.internal.catalog.commands.CreateSystemViewCommand;
import org.apache.ignite.internal.catalog.commands.CreateTableCommand;
import org.apache.ignite.internal.catalog.commands.CreateTableCommandBuilder;
import org.apache.ignite.internal.catalog.commands.CreateZoneCommand;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.MakeIndexAvailableCommand;
import org.apache.ignite.internal.catalog.commands.StartBuildingIndexCommand;
import org.apache.ignite.internal.catalog.commands.StorageProfileParams;
import org.apache.ignite.internal.catalog.commands.TableHashPrimaryKey;
import org.apache.ignite.internal.catalog.commands.TablePrimaryKey;
import org.apache.ignite.internal.catalog.commands.TableSortedPrimaryKey;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor.SystemViewType;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.schema.DefaultValueGenerator;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.cache.CaffeineCacheFactory;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;


/** Tests for {@link SqlSchemaManagerImpl}. */
@ExtendWith(MockitoExtension.class)
public class SqlSchemaManagerImplTest extends BaseIgniteAbstractTest {
    private static final String PUBLIC_SCHEMA_NAME = "PUBLIC";
    private static final String SYSTEM_SCHEMA_NAME = "SYSTEM";

    private CatalogManager catalogManager;
    private SqlSchemaManagerImpl sqlSchemaManager;

    @BeforeEach
    void init() {
        catalogManager = CatalogTestUtils.createCatalogManagerWithTestUpdateLog("test", new HybridClockImpl());
        sqlSchemaManager = new SqlSchemaManagerImpl(catalogManager, CaffeineCacheFactory.INSTANCE, 200);

        assertThat(catalogManager.startAsync(), willCompleteSuccessfully());
    }

    @AfterEach
    void cleanup() {
        assertThat(catalogManager.stopAsync(), willCompleteSuccessfully());
    }

    @Test
    void exceptionWillBeThrownIfTableNotFoundById() {
        await(catalogManager.execute(List.of(
                createDummyTable("T1")
        )));

        int versionAfter = catalogManager.latestCatalogVersion();

        // just to fill up cache
        sqlSchemaManager.schema(versionAfter);

        int nonExistingTableId = Integer.MAX_VALUE;

        assertThat(catalogManager.table(nonExistingTableId, versionAfter), nullValue());

        assertThrowsWithCause(
                () -> sqlSchemaManager.table(versionAfter, nonExistingTableId),
                IgniteInternalException.class,
                "Table with given id not found"
        );
    }

    @Test
    void testMultipleSchemas() {
        int versionBefore = catalogManager.latestCatalogVersion();
        await(catalogManager.execute(List.of(
                createDummyTable("T1"),
                createDummySystemView("V1", SystemViewType.NODE)
        )));

        int versionAfter = catalogManager.latestCatalogVersion();
        assertThat(versionAfter, equalTo(versionBefore + 1));

        SchemaPlus rootSchema = sqlSchemaManager.schema(versionAfter);
        assertNotNull(rootSchema);

        assertNotNull(rootSchema.getSubSchema(PUBLIC_SCHEMA_NAME));
        assertNotNull(rootSchema.getSubSchema(SYSTEM_SCHEMA_NAME));
    }

    /** Basic schema with several tables. */
    @Test
    public void testBasicSchema() {
        int versionBefore = catalogManager.latestCatalogVersion();
        await(catalogManager.execute(List.of(
                createDummyTable("T1"),
                createDummyTable("T2")
        )));

        int versionAfter = catalogManager.latestCatalogVersion();
        assertThat(versionAfter, equalTo(versionBefore + 1));

        SchemaPlus rootSchema = sqlSchemaManager.schema(versionAfter);
        assertNotNull(rootSchema);

        SchemaPlus schemaPlus = rootSchema.getSubSchema(PUBLIC_SCHEMA_NAME);
        assertNotNull(schemaPlus);

        IgniteSchema schema = unwrapSchema(schemaPlus);

        assertThat(schema.version(), equalTo(versionAfter));

        CatalogSchemaDescriptor schemaDescriptor = catalogManager.schema(PUBLIC_SCHEMA_NAME, versionAfter);

        assertThat(schemaDescriptor, notNullValue());
        assertThat(schema.getName(), equalTo(schemaDescriptor.name()));

        for (CatalogTableDescriptor tableDescriptor : schemaDescriptor.tables()) {
            int zoneId = tableDescriptor.zoneId();
            CatalogZoneDescriptor zoneDescriptor = catalogManager.zone(zoneId, versionAfter);
            assertNotNull(zoneDescriptor, "Zone does not exist: " + zoneId);

            Table table = schema.getTable(tableDescriptor.name());
            assertThat(table, notNullValue());

            IgniteTable igniteTable = assertInstanceOf(IgniteTable.class, table);

            assertEquals(zoneDescriptor.partitions(), igniteTable.partitions(),
                    "Number of partitions is not correct: " + tableDescriptor.name());

            assertEquals(CatalogUtils.DEFAULT_PARTITION_COUNT, igniteTable.partitions(),
                    "Number of partitions is not correct: " + tableDescriptor.name());
        }
    }

    /** Create a table with a zone. */
    @Test
    public void testTableWithZone() {
        int partitions = 10;

        await(catalogManager.execute(CreateZoneCommand.builder()
                .partitions(partitions)
                .zoneName("ABC")
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build()))
                .build())
        );

        await(catalogManager.execute(CreateTableCommand.builder()
                .schemaName(PUBLIC_SCHEMA_NAME)
                .tableName("T")
                .columns(List.of(
                        ColumnParams.builder().name("ID").type(ColumnType.INT32).nullable(false).build(),
                        ColumnParams.builder().name("VAL").type(ColumnType.INT32).build()
                ))
                .primaryKey(primaryKey("ID"))
                .zone("ABC")
                .build()));

        int version = catalogManager.latestCatalogVersion();

        SchemaPlus rootSchema = sqlSchemaManager.schema(version);
        assertNotNull(rootSchema);

        SchemaPlus schemaPlus = rootSchema.getSubSchema(PUBLIC_SCHEMA_NAME);
        assertNotNull(schemaPlus);

        IgniteSchema schema = unwrapSchema(schemaPlus);
        Table table = schema.getTable("T");
        assertNotNull(table);

        IgniteTable igniteTable = assertInstanceOf(IgniteTable.class, table);
        assertEquals(partitions, igniteTable.partitions());
    }

    /**
     * Table column types.
     */
    @ParameterizedTest
    @MethodSource("columnTypes")
    public void testTableColumns(ColumnType columnType, int precision, int scale) {
        int versionBefore = catalogManager.latestCatalogVersion();
        await(catalogManager.execute(List.of(
                CreateTableCommand.builder()
                        .schemaName(PUBLIC_SCHEMA_NAME)
                        .tableName("TEST")
                        .columns(List.of(
                                ColumnParams.builder().name("ID").type(ColumnType.INT32).nullable(false).build(),
                                column("VAL_NULLABLE", columnType, precision, scale, true),
                                column("VAL_NOT_NULLABLE", columnType, precision, scale, false)
                        ))
                        .primaryKey(primaryKey("ID"))
                        .zone("Default")
                        .build()
        )));

        int versionAfter = catalogManager.latestCatalogVersion();
        assertThat(versionAfter, equalTo(versionBefore + 1));

        SchemaPlus rootSchema = sqlSchemaManager.schema(versionAfter);
        assertNotNull(rootSchema);

        SchemaPlus schemaPlus = rootSchema.getSubSchema(PUBLIC_SCHEMA_NAME);
        assertNotNull(schemaPlus);

        IgniteTable table = getTable(unwrapSchema(schemaPlus), "TEST");

        CatalogTableDescriptor tableDescriptor = catalogManager.table(table.id(), versionAfter);

        assertThat(tableDescriptor, notNullValue());

        TableDescriptor descriptor = table.descriptor();
        assertEquals(tableDescriptor.columns().size(), descriptor.columnsCount(), "column count");

        for (int i = 0; i < descriptor.columnsCount(); i++) {
            CatalogTableColumnDescriptor expectedColumnDescriptor = tableDescriptor.columns().get(i);
            ColumnDescriptor actualColumnDescriptor = descriptor.columnDescriptor(i);

            checkColumns(actualColumnDescriptor, i, expectedColumnDescriptor);
        }
    }

    private static void checkColumns(
            ColumnDescriptor actualColumnDescriptor,
            int expectedIndex,
            CatalogTableColumnDescriptor expectedColumnDescriptor
    ) {
        assertThat(actualColumnDescriptor.logicalIndex(), equalTo(expectedIndex));
        assertThat(actualColumnDescriptor.name(), equalTo(expectedColumnDescriptor.name()));
        assertThat(actualColumnDescriptor.nullable(), equalTo(expectedColumnDescriptor.nullable()));
        assertThat(
                actualColumnDescriptor.physicalType(),
                equalTo(columnType2NativeType(
                        expectedColumnDescriptor.type(),
                        expectedColumnDescriptor.precision(),
                        expectedColumnDescriptor.scale(),
                        expectedColumnDescriptor.length()
                ))
        );
    }

    /**
     * Column default value constraint.
     */
    @Test
    public void testTableDefaultValue() {
        int versionBefore = catalogManager.latestCatalogVersion();
        await(catalogManager.execute(List.of(
                CreateTableCommand.builder()
                        .schemaName(PUBLIC_SCHEMA_NAME)
                        .tableName("TEST")
                        .columns(List.of(
                                ColumnParams.builder().name("C1").type(ColumnType.INT32).nullable(false).build(),
                                ColumnParams.builder().name("C2").type(ColumnType.INT32)
                                        .defaultValue(DefaultValue.constant(null)).build(),
                                ColumnParams.builder().name("C3").type(ColumnType.INT32)
                                        .defaultValue(DefaultValue.constant(1)).build(),
                                ColumnParams.builder().name("C4").type(ColumnType.UUID)
                                        .defaultValue(DefaultValue.functionCall(DefaultValueGenerator.RAND_UUID.name())).build()
                        ))
                        .primaryKey(primaryKey("C1", "C4"))
                        .zone("Default")
                        .build()
        )));

        int versionAfter = catalogManager.latestCatalogVersion();
        assertThat(versionAfter, equalTo(versionBefore + 1));

        SchemaPlus rootSchema = sqlSchemaManager.schema(versionAfter);
        assertNotNull(rootSchema);

        SchemaPlus schemaPlus = rootSchema.getSubSchema(PUBLIC_SCHEMA_NAME);
        assertNotNull(schemaPlus);

        IgniteTable table = getTable(unwrapSchema(schemaPlus), "TEST");

        ColumnDescriptor c1 = table.descriptor().columnDescriptor("C1");
        assertNull(c1.defaultValue());
        assertEquals(DefaultValueStrategy.DEFAULT_NULL, c1.defaultStrategy());

        ColumnDescriptor c2 = table.descriptor().columnDescriptor("C2");
        assertNull(c2.defaultValue());
        assertEquals(DefaultValueStrategy.DEFAULT_NULL, c2.defaultStrategy());

        ColumnDescriptor c3 = table.descriptor().columnDescriptor("C3");
        assertEquals(1, c3.defaultValue());
        assertEquals(DefaultValueStrategy.DEFAULT_CONSTANT, c3.defaultStrategy());

        ColumnDescriptor c4 = table.descriptor().columnDescriptor("C4");
        assertNotNull(c4.defaultValue());
        assertEquals(DefaultValueStrategy.DEFAULT_COMPUTED, c4.defaultStrategy());
    }

    /**
     * Table with primary key but w/o colocation key columns are legal.
     */
    @ParameterizedTest
    @MethodSource("primaryKeyTypesColumnsC1C3")
    public void testTableWithPrimaryKey(TablePrimaryKey primaryKey) {
        int versionBefore = catalogManager.latestCatalogVersion();
        await(catalogManager.execute(List.of(
                CreateTableCommand.builder()
                        .schemaName(PUBLIC_SCHEMA_NAME)
                        .tableName("TEST")
                        .columns(List.of(
                                ColumnParams.builder().name("C1").type(ColumnType.INT32).nullable(false).build(),
                                ColumnParams.builder().name("C2").type(ColumnType.INT32)
                                        .defaultValue(DefaultValue.constant(null)).build(),
                                ColumnParams.builder().name("C3").type(ColumnType.UUID)
                                        .defaultValue(DefaultValue.functionCall(DefaultValueGenerator.RAND_UUID.name())).build(),
                                ColumnParams.builder().name("C4").type(ColumnType.INT8).nullable(true).build()
                        ))
                        .primaryKey(primaryKey)
                        .zone("Default")
                        .build()
        )));

        int versionAfter = catalogManager.latestCatalogVersion();
        assertThat(versionAfter, equalTo(versionBefore + 1));

        SchemaPlus rootSchema = sqlSchemaManager.schema(versionAfter);
        assertNotNull(rootSchema);

        SchemaPlus schemaPlus = rootSchema.getSubSchema(PUBLIC_SCHEMA_NAME);
        assertNotNull(schemaPlus);

        IgniteTable table = getTable(unwrapSchema(schemaPlus), "TEST");

        assertTrue(table.descriptor().columnDescriptor(0).key());
        assertFalse(table.descriptor().columnDescriptor(1).key());
        assertTrue(table.descriptor().columnDescriptor(2).key());
        assertFalse(table.descriptor().columnDescriptor(3).key());
    }

    private static Stream<TablePrimaryKey> primaryKeyTypesColumnsC1C3() {
        return Stream.of(
                TableHashPrimaryKey.builder()
                        .columns(List.of("C1", "C3"))
                        .build(),
                TableSortedPrimaryKey.builder()
                        .columns(List.of("C1", "C3"))
                        .collations(List.of(CatalogColumnCollation.ASC_NULLS_LAST, CatalogColumnCollation.DESC_NULLS_FIRST))
                        .build()
        );
    }

    /**
     * Table distribution.
     */
    @Test
    public void testTableDistribution() {
        Function<CreateTableCommandBuilder, CreateTableCommandBuilder> tableBase = builder -> builder
                .schemaName(PUBLIC_SCHEMA_NAME)
                .columns(List.of(
                        ColumnParams.builder().name("C1").type(ColumnType.INT32).build(),
                        ColumnParams.builder().name("C2").type(ColumnType.INT32).build(),
                        ColumnParams.builder().name("C3").type(ColumnType.INT32).build(),
                        ColumnParams.builder().name("C4").type(ColumnType.INT32).build()
                ))
                .primaryKey(primaryKey("C1", "C2", "C3", "C4"))
                .zone("Default");

        int versionBefore = catalogManager.latestCatalogVersion();
        await(catalogManager.execute(List.of(
                tableBase.apply(CreateTableCommand.builder())
                        .tableName("T1")
                        .colocationColumns(List.of("C2"))
                        .build(),
                tableBase.apply(CreateTableCommand.builder())
                        .tableName("T2")
                        .colocationColumns(List.of("C4", "C2"))
                        .build(),
                tableBase.apply(CreateTableCommand.builder())
                        .tableName("T3")
                        .colocationColumns(List.of("C3", "C2", "C1"))
                        .build()
        )));

        int versionAfter = catalogManager.latestCatalogVersion();
        assertThat(versionAfter, equalTo(versionBefore + 1));

        SchemaPlus rootSchema = sqlSchemaManager.schema(versionAfter);
        assertNotNull(rootSchema);

        SchemaPlus schemaPlus = rootSchema.getSubSchema(PUBLIC_SCHEMA_NAME);
        assertNotNull(schemaPlus);

        {
            IgniteTable table = getTable(unwrapSchema(schemaPlus), "T1");
            IgniteDistribution distribution = table.descriptor().distribution();

            assertThat(distribution, equalTo(IgniteDistributions.affinity(List.of(1), table.id(), table.id())));
        }

        {
            IgniteTable table = getTable(unwrapSchema(schemaPlus), "T2");
            IgniteDistribution distribution = table.descriptor().distribution();

            assertThat(distribution, equalTo(IgniteDistributions.affinity(List.of(3, 1), table.id(), table.id())));
        }

        {
            IgniteTable table = getTable(unwrapSchema(schemaPlus), "T3");
            IgniteDistribution distribution = table.descriptor().distribution();

            assertThat(distribution, equalTo(IgniteDistributions.affinity(List.of(2, 1, 0), table.id(), table.id())));
        }
    }

    @Test
    public void testHashIndex() {
        int versionBefore = catalogManager.latestCatalogVersion();
        await(catalogManager.execute(List.of(
                createDummyTable("T1"),
                createHashIndex("T1", "VAL1_IDX", "VAL1")
        )));

        {
            int versionAfter = catalogManager.latestCatalogVersion();
            assertThat(versionAfter, equalTo(versionBefore + 1));

            SchemaPlus rootSchema = sqlSchemaManager.schema(versionAfter);
            assertNotNull(rootSchema);

            SchemaPlus schemaPlus = rootSchema.getSubSchema(PUBLIC_SCHEMA_NAME);
            assertNotNull(schemaPlus);

            IgniteIndex index = findIndex(unwrapSchema(schemaPlus), "T1", "VAL1_IDX");
            assertNull(index, "Index should not be available");
        }

        makeIndexAvailable("VAL1_IDX");

        {
            int versionAfter = catalogManager.latestCatalogVersion();
            assertThat(versionAfter, equalTo(versionBefore + 2));

            SchemaPlus rootSchema = sqlSchemaManager.schema(versionAfter);
            assertNotNull(rootSchema);

            SchemaPlus schemaPlus = rootSchema.getSubSchema(PUBLIC_SCHEMA_NAME);
            assertNotNull(schemaPlus);

            IgniteIndex index = findIndex(unwrapSchema(schemaPlus), "T1", "VAL1_IDX");
            assertNotNull(index);

            assertThat(index.name(), equalTo("VAL1_IDX"));
            assertThat(index.type(), equalTo(Type.HASH));
            assertThat(index.collation(), equalTo(RelCollations.of(
                    new RelFieldCollation(1, Direction.CLUSTERED, NullDirection.UNSPECIFIED)
            )));
        }
    }

    @Test
    public void testSortedIndex() {
        int versionBefore = catalogManager.latestCatalogVersion();
        await(catalogManager.execute(List.of(
                createDummyTable("T1"),
                createSortedIndex("T1", "IDX1", List.of("VAL1", "VAL2"),
                        List.of(CatalogColumnCollation.ASC_NULLS_FIRST, CatalogColumnCollation.ASC_NULLS_LAST)),
                createSortedIndex("T1", "IDX2", List.of("VAL1", "VAL2"),
                        List.of(CatalogColumnCollation.DESC_NULLS_FIRST, CatalogColumnCollation.DESC_NULLS_LAST))
        )));

        {
            int versionAfter = catalogManager.latestCatalogVersion();
            assertThat(versionAfter, equalTo(versionBefore + 1));

            SchemaPlus rootSchema = sqlSchemaManager.schema(versionAfter);
            assertNotNull(rootSchema);

            SchemaPlus schemaPlus = rootSchema.getSubSchema(PUBLIC_SCHEMA_NAME);
            assertNotNull(schemaPlus);

            IgniteIndex index1 = findIndex(unwrapSchema(schemaPlus), "T1", "IDX1");
            assertNull(index1);

            IgniteIndex index2 = findIndex(unwrapSchema(schemaPlus), "T1", "IDX2");
            assertNull(index2);
        }

        makeIndexAvailable("IDX1");

        {
            int versionAfter = catalogManager.latestCatalogVersion();
            assertThat(versionAfter, equalTo(versionBefore + 2));

            SchemaPlus rootSchema = sqlSchemaManager.schema(versionAfter);
            assertNotNull(rootSchema);

            SchemaPlus schemaPlus = rootSchema.getSubSchema(PUBLIC_SCHEMA_NAME);
            assertNotNull(schemaPlus);

            IgniteIndex index = findIndex(unwrapSchema(schemaPlus), "T1", "IDX1");
            assertNotNull(index);

            assertThat(index.name(), equalTo("IDX1"));
            assertThat(index.type(), equalTo(Type.SORTED));
            assertThat(index.collation(), equalTo(RelCollations.of(
                    new RelFieldCollation(1, Direction.ASCENDING, NullDirection.FIRST),
                    new RelFieldCollation(2, Direction.ASCENDING, NullDirection.LAST)
            )));
        }

        makeIndexAvailable("IDX2");

        {
            int versionAfter = catalogManager.latestCatalogVersion();
            assertThat(versionAfter, equalTo(versionBefore + 3));

            SchemaPlus rootSchema = sqlSchemaManager.schema(versionAfter);
            assertNotNull(rootSchema);

            SchemaPlus schemaPlus = rootSchema.getSubSchema(PUBLIC_SCHEMA_NAME);
            assertNotNull(schemaPlus);

            IgniteIndex index = findIndex(unwrapSchema(schemaPlus), "T1", "IDX2");
            assertNotNull(index);

            assertThat(index.name(), equalTo("IDX2"));
            assertThat(index.type(), equalTo(Type.SORTED));
            assertThat(index.collation(), equalTo(RelCollations.of(
                    new RelFieldCollation(1, Direction.DESCENDING, NullDirection.FIRST),
                    new RelFieldCollation(2, Direction.DESCENDING, NullDirection.LAST)
            )));
        }
    }

    private void makeIndexAvailable(String name) {
        Map<String, CatalogIndexDescriptor> indices = catalogManager.indexes(catalogManager.latestCatalogVersion())
                .stream().collect(Collectors.toMap(CatalogIndexDescriptor::name, Function.identity()));

        CatalogIndexDescriptor indexDescriptor = indices.get(name);
        assertNotNull(indexDescriptor, indices.toString());

        CatalogCommand startBuilding = StartBuildingIndexCommand.builder().indexId(indexDescriptor.id()).build();
        CatalogCommand makeAvailable = MakeIndexAvailableCommand.builder().indexId(indexDescriptor.id()).build();

        await(catalogManager.execute(List.of(startBuilding, makeAvailable)));
    }

    @ParameterizedTest
    @MethodSource("systemViewDistributions")
    public void testBasicView(SystemViewType viewType, IgniteDistribution distribution) {
        int versionBefore = catalogManager.latestCatalogVersion();
        await(catalogManager.execute(List.of(
                createDummySystemView("V1", SystemViewType.NODE),
                createDummySystemView("V2", SystemViewType.CLUSTER)
        )));

        int versionAfter = catalogManager.latestCatalogVersion();
        assertThat(versionAfter, equalTo(versionBefore + 1));

        SchemaPlus rootSchema = sqlSchemaManager.schema(versionAfter);
        assertNotNull(rootSchema);

        SchemaPlus schemaPlus = rootSchema.getSubSchema(SYSTEM_SCHEMA_NAME);
        assertNotNull(schemaPlus);

        {
            IgniteSystemView systemView = getSystemView(unwrapSchema(schemaPlus), "V1");

            assertThat(systemView.name(), equalTo("V1"));
            assertThat(systemView.distribution(), equalTo(IgniteDistributions.identity(0)));
        }

        {
            IgniteSystemView systemView = getSystemView(unwrapSchema(schemaPlus), "V2");

            assertThat(systemView.name(), equalTo("V2"));
            assertThat(systemView.distribution(), equalTo(IgniteDistributions.single()));
        }
    }

    @ParameterizedTest
    @MethodSource("systemViewColumnTypes")
    public void testViewColumns(SystemViewType viewType, ColumnType columnType, int precision, int scale) {
        int versionBefore = catalogManager.latestCatalogVersion();
        await(catalogManager.execute(List.of(
                CreateSystemViewCommand.builder()
                        .name("V1")
                        .columns(List.of(
                                ColumnParams.builder().name("ID").type(ColumnType.INT32).nullable(false).build(),
                                column("VAL_NULLABLE", columnType, precision, scale, true),
                                column("VAL_NOT_NULLABLE", columnType, precision, scale, false)
                        ))
                        .type(SystemViewType.CLUSTER)
                        .build()
        )));

        int versionAfter = catalogManager.latestCatalogVersion();
        assertThat(versionAfter, equalTo(versionBefore + 1));

        SchemaPlus rootSchema = sqlSchemaManager.schema(versionAfter);
        assertNotNull(rootSchema);

        SchemaPlus schemaPlus = rootSchema.getSubSchema(SYSTEM_SCHEMA_NAME);
        assertNotNull(schemaPlus);

        CatalogSchemaDescriptor schemaDescriptor = catalogManager.schema(SYSTEM_SCHEMA_NAME, versionAfter);
        assertNotNull(schemaDescriptor);

        CatalogSystemViewDescriptor viewDescriptor = schemaDescriptor.systemView("V1");
        assertNotNull(viewDescriptor);

        IgniteSystemView systemView = getSystemView(unwrapSchema(schemaPlus), "V1");

        TableDescriptor descriptor = systemView.descriptor();
        assertEquals(viewDescriptor.columns().size(), descriptor.columnsCount(), "column count");

        for (int i = 0; i < descriptor.columnsCount(); i++) {
            CatalogTableColumnDescriptor expectedColumnDescriptor = viewDescriptor.columns().get(i);
            ColumnDescriptor actualColumnDescriptor = descriptor.columnDescriptor(i);

            checkColumns(actualColumnDescriptor, i, expectedColumnDescriptor);
        }
    }

    private static Stream<Arguments> systemViewColumnTypes() {
        List<Arguments> allArgs = new ArrayList<>();

        for (SystemViewType type : SystemViewType.values()) {
            columnTypes().map(args -> {
                Object[] vals = args.get();

                Object[] newVals = new Object[vals.length + 1];
                newVals[0] = type;

                System.arraycopy(vals, 0, newVals, 1, vals.length);

                return Arguments.of(newVals);
            }).forEach(allArgs::add);
        }

        return allArgs.stream();
    }

    private static Stream<Arguments> systemViewDistributions() {
        return Stream.of(
                Arguments.of(SystemViewType.NODE, IgniteDistributions.identity(0)),
                Arguments.of(SystemViewType.CLUSTER, IgniteDistributions.single())
        );
    }


    private static IgniteSystemView getSystemView(IgniteSchema schema, String name) {
        Table systemViewTable = schema.getTable(name);
        assertNotNull(systemViewTable);

        IgniteSystemView systemView = assertInstanceOf(IgniteSystemView.class, systemViewTable);
        assertEquals(systemView.name(), name);

        return systemView;
    }

    private static IgniteSchema unwrapSchema(SchemaPlus schemaPlus) {
        IgniteSchema igniteSchema = schemaPlus.unwrap(IgniteSchema.class);
        assertNotNull(igniteSchema);
        return igniteSchema;
    }

    private static IgniteTable getTable(IgniteSchema schema, String name) {
        IgniteTable table = (IgniteTable) schema.getTable(name);
        assertNotNull(table);
        return table;
    }

    private static @Nullable IgniteIndex findIndex(IgniteSchema schema, String tableName, String indexName) {
        IgniteTable table = (IgniteTable) schema.getTable(tableName);
        assertNotNull(table);
        return table.indexes().get(indexName);
    }

    private static Stream<Arguments> columnTypes() {
        return Stream.of(
                Arguments.of(ColumnType.BOOLEAN, -1, -1),
                Arguments.of(ColumnType.INT8, -1, -1),
                Arguments.of(ColumnType.INT16, -1, -1),
                Arguments.of(ColumnType.INT32, -1, -1),
                Arguments.of(ColumnType.INT64, -1, -1),
                Arguments.of(ColumnType.FLOAT, -1, -1),
                Arguments.of(ColumnType.DOUBLE, -1, -1),
                Arguments.of(ColumnType.DECIMAL, 4, 0),
                Arguments.of(ColumnType.DECIMAL, 4, 2),
                Arguments.of(ColumnType.NUMBER, 4, -1),
                Arguments.of(ColumnType.STRING, 40, -1),
                Arguments.of(ColumnType.BYTE_ARRAY, 40, -1),
                Arguments.of(ColumnType.DATE, -1, -1),
                Arguments.of(ColumnType.TIME, 2, -1),
                Arguments.of(ColumnType.DATETIME, 2, -1),
                Arguments.of(ColumnType.TIMESTAMP, 2, -1),
                Arguments.of(ColumnType.UUID, -1, -1),
                Arguments.of(ColumnType.BITMASK, 2, -1)
        );
    }

    private static CatalogCommand createDummyTable(String name) {
        return CreateTableCommand.builder()
                .schemaName(PUBLIC_SCHEMA_NAME)
                .tableName(name)
                .columns(List.of(
                        ColumnParams.builder().name("ID").type(ColumnType.INT32).nullable(false).build(),
                        ColumnParams.builder().name("VAL1").type(ColumnType.INT32).nullable(false).build(),
                        ColumnParams.builder().name("VAL2").type(ColumnType.INT32).nullable(false).build()
                ))
                .primaryKey(TableHashPrimaryKey.builder()
                        .columns(List.of("ID"))
                        .build())
                .zone("Default")
                .build();
    }

    private static CatalogCommand createDummySystemView(String name, SystemViewType type) {
        return CreateSystemViewCommand.builder()
                .name(name)
                .columns(List.of(
                        ColumnParams.builder().name("NODE").type(ColumnType.STRING).length(256).build(),
                        ColumnParams.builder().name("C1").type(ColumnType.INT32).nullable(false).build(),
                        ColumnParams.builder().name("C2").type(ColumnType.INT32).nullable(false).build()
                ))
                .type(type)
                .build();
    }

    private static CatalogCommand createHashIndex(String tableName, String indexName, String... cols) {
        return CreateHashIndexCommand.builder()
                .schemaName(PUBLIC_SCHEMA_NAME)
                .tableName(tableName)
                .indexName(indexName)
                .unique(false)
                .columns(Arrays.asList(cols))
                .build();
    }

    private static CatalogCommand createSortedIndex(String tableName, String indexName,
            List<String> cols, List<CatalogColumnCollation> collations) {
        return CreateSortedIndexCommand.builder()
                .schemaName(PUBLIC_SCHEMA_NAME)
                .tableName(tableName)
                .indexName(indexName)
                .unique(false)
                .columns(cols)
                .collations(collations)
                .build();
    }

    private static ColumnParams column(String name, ColumnType columnType, int precision, int scale, boolean nullable) {
        ColumnParams.Builder builder = ColumnParams.builder()
                .name(name)
                .type(columnType)
                .nullable(nullable);

        switch (columnType) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case UUID:
            case BOOLEAN:
                break;
            case NUMBER:
            case TIME:
            case DATETIME:
            case TIMESTAMP:
                builder.precision(precision);
                break;
            case DECIMAL:
                builder.precision(precision);
                builder.scale(scale);
                break;
            case STRING:
            case BYTE_ARRAY:
            case BITMASK:
                builder.length(precision);
                break;
            default:
                throw new IllegalArgumentException("Unsupported native type: " + columnType);
        }

        return builder.build();
    }

    private static TablePrimaryKey primaryKey(String column, String... columns) {
        List<String> pkColumns = new ArrayList<>();
        pkColumns.add(column);
        pkColumns.addAll(Arrays.asList(columns));

        return TableHashPrimaryKey.builder()
                .columns(pkColumns)
                .build();
    }
}
