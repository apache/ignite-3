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

import static org.apache.ignite.internal.catalog.CatalogManagerImpl.INITIAL_CAUSALITY_TOKEN;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor.SystemViewType;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.schema.DefaultValueGenerator;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;


/**
 * Tests for {@link CatalogSqlSchemaManager}.
 */
@ExtendWith(MockitoExtension.class)
public class CatalogSqlSchemaManagerTest extends BaseIgniteAbstractTest {

    private static final AtomicInteger ID = new AtomicInteger();

    @Mock
    private CatalogManager catalogManager;

    /** Basic schema with several tables. */
    @Test
    public void testBasicSchema() {
        TestSchema testSchema = new TestSchema("TEST");
        testSchema.version = 2000;
        testSchema.timestamp = 111111L;

        TestTable t1 = new TestTable("T1");
        t1.addColumn("c1", ColumnType.INT32);

        TestTable t2 = new TestTable("T2");
        t2.addColumn("c1", ColumnType.STRING);

        testSchema.tables.add(t1);
        testSchema.tables.add(t2);

        testSchema.init(catalogManager);

        SqlSchemaManager sqlSchemaManager = newSchemaManager();
        SchemaPlus schemaPlus = sqlSchemaManager.schema(testSchema.name, testSchema.timestamp);
        IgniteSchema schema = unwrapSchema(schemaPlus);

        assertEquals(testSchema.name, schema.getName());
        assertEquals(testSchema.version, schema.version());

        assertNotNull(schema.getTable(t1.name));
        assertNotNull(schema.getTable(t2.name));
    }

    /**
     * Table column types.
     */
    @ParameterizedTest
    @MethodSource("columnTypes")
    public void testTableColumns(ColumnType columnType, int precision, int scale, boolean hasNativeType) {
        TestTable testTable = new TestTable("TEST");

        testTable.addColumn("c1_nullable", columnType, precision, scale);
        testTable.addColumn("c1_not_nullable", columnType, precision, scale);

        testTable.notNull("c1_not_nullable");

        TestSchema testSchema = new TestSchema();
        testSchema.tables.add(testTable);
        testSchema.init(catalogManager);

        SqlSchemaManager sqlSchemaManager = newSchemaManager();
        SchemaPlus schemaPlus = sqlSchemaManager.schema(testSchema.name, testSchema.timestamp);
        IgniteSchema schema = unwrapSchema(schemaPlus);

        IgniteTable table = getTable(schema, testTable);

        assertEquals(testTable.id, table.id());

        TableDescriptor descriptor = table.descriptor();
        assertEquals(testTable.columns.size(), descriptor.columnsCount(), "column count");

        checkColumns(columnType, precision, scale, hasNativeType, descriptor);
    }

    private static void checkColumns(ColumnType columnType, int precision, int scale, boolean hasNativeType, TableDescriptor descriptor) {
        ColumnDescriptor c1 = descriptor.columnDescriptor(0);
        assertEquals(0, c1.logicalIndex());
        assertTrue(c1.nullable());

        ColumnDescriptor c2 = descriptor.columnDescriptor(1);
        assertEquals(1, c2.logicalIndex());
        assertFalse(c2.nullable());

        if (hasNativeType) {
            NativeType nativeType = c1.physicalType();
            NativeType expectedNativeType = TypeUtils.columnType2NativeType(columnType, precision, scale, precision);

            assertEquals(expectedNativeType, nativeType);
        } else {
            IllegalArgumentException t = assertThrows(IllegalArgumentException.class, c1::physicalType);
            assertThat(t.getMessage(), containsString("No NativeType for type"));
        }
    }

    /** Empty default schema. */
    @Test
    public void testDefaultSchema() {
        TestSchema testSchema = new TestSchema();

        testSchema.init(catalogManager);

        SqlSchemaManager sqlSchemaManager = newSchemaManager();

        {
            SchemaPlus schemaPlus = sqlSchemaManager.schema(testSchema.name, testSchema.timestamp);
            IgniteSchema schema = unwrapSchema(schemaPlus);

            assertEquals(DEFAULT_SCHEMA_NAME, schema.getName());
            assertEquals(testSchema.version, schema.version());
        }

        {
            SchemaPlus schemaPlus = sqlSchemaManager.schema(null, testSchema.timestamp);
            IgniteSchema schema = unwrapSchema(schemaPlus);

            assertEquals(DEFAULT_SCHEMA_NAME, schema.getName());
            assertEquals(testSchema.version, schema.version());
        }
    }

    /**
     * Column default value constraint.
     */
    @Test
    public void testTableDefaultValue() {
        TestTable testTable = new TestTable("TEST");

        testTable.addColumn("c1", ColumnType.INT32);
        testTable.addColumn("c2", ColumnType.INT32);
        testTable.addColumn("c3", ColumnType.INT32);
        testTable.addColumn("c4", ColumnType.STRING);

        testTable.defaultValueMap.put("c2", DefaultValue.constant(null));
        testTable.defaultValueMap.put("c3", DefaultValue.constant(1));
        testTable.defaultValueMap.put("c4", DefaultValue.functionCall(DefaultValueGenerator.GEN_RANDOM_UUID.name()));

        TestSchema testSchema = new TestSchema();
        testSchema.tables.add(testTable);
        testSchema.init(catalogManager);

        SqlSchemaManager sqlSchemaManager = newSchemaManager();
        SchemaPlus schemaPlus = sqlSchemaManager.schema(testSchema.name, testSchema.timestamp);
        IgniteSchema schema = unwrapSchema(schemaPlus);

        IgniteTable table = getTable(schema, testTable);

        assertEquals(testTable.id, table.id());

        ColumnDescriptor c1 = table.descriptor().columnDescriptor("c1");
        assertNull(c1.defaultValue());
        assertEquals(DefaultValueStrategy.DEFAULT_NULL, c1.defaultStrategy());

        ColumnDescriptor c2 = table.descriptor().columnDescriptor("c2");
        assertNull(c2.defaultValue());
        assertEquals(DefaultValueStrategy.DEFAULT_NULL, c2.defaultStrategy());

        ColumnDescriptor c3 = table.descriptor().columnDescriptor("c3");
        assertEquals(1, c3.defaultValue());
        assertEquals(DefaultValueStrategy.DEFAULT_CONSTANT, c3.defaultStrategy());

        ColumnDescriptor c4 = table.descriptor().columnDescriptor("c4");
        assertNotNull(c4.defaultValue());
        assertEquals(DefaultValueStrategy.DEFAULT_COMPUTED, c4.defaultStrategy());
    }

    /**
     * Table with neither primary key nor colocation key columns are legal.
     */
    @Test
    public void testTableWithoutPrimaryKeyColoKey() {
        TestTable testTable = new TestTable("TEST");

        testTable.addColumn("c1", ColumnType.INT8);
        testTable.addColumn("c2", ColumnType.INT16);

        TestSchema testSchema = new TestSchema();
        testSchema.tables.add(testTable);
        testSchema.init(catalogManager);

        SqlSchemaManager sqlSchemaManager = newSchemaManager();
        SchemaPlus schemaPlus = sqlSchemaManager.schema(testSchema.name, testSchema.timestamp);
        IgniteSchema schema = unwrapSchema(schemaPlus);

        IgniteTable table = (IgniteTable) schema.getTable(testTable.name);
        assertNotNull(table);

        assertFalse(table.descriptor().columnDescriptor(0).key());
        assertFalse(table.descriptor().columnDescriptor(1).key());
    }

    /**
     * Table with primary key but w/o colocation key columns are legal.
     */
    @Test
    public void testTableWithPrimaryKeyWithoutColoKey() {
        TestTable testTable = new TestTable("TEST");

        testTable.addColumn("c1", ColumnType.INT8);
        testTable.addColumn("c2", ColumnType.INT16);

        testTable.primaryKey("c1");
        testTable.notNull("c1");

        TestSchema testSchema = new TestSchema();
        testSchema.tables.add(testTable);
        testSchema.init(catalogManager);

        SqlSchemaManager sqlSchemaManager = newSchemaManager();
        SchemaPlus schemaPlus = sqlSchemaManager.schema(testSchema.name, testSchema.timestamp);
        IgniteSchema schema = unwrapSchema(schemaPlus);

        IgniteTable table = (IgniteTable) schema.getTable(testTable.name);
        assertNotNull(table);

        assertTrue(table.descriptor().columnDescriptor(0).key());
        assertFalse(table.descriptor().columnDescriptor(0).nullable());
        assertFalse(table.descriptor().columnDescriptor(1).key());
    }

    /**
     * Table distribution.
     */
    @Test
    public void testTableDistribution() {
        TestTable testTable = new TestTable("TEST");

        testTable.addColumn("c1", ColumnType.INT8);
        testTable.addColumn("c2", ColumnType.INT16);
        testTable.addColumn("c3", ColumnType.INT32);
        testTable.addColumn("c4", ColumnType.INT64);
        testTable.addColumn("c5", ColumnType.FLOAT);
        testTable.addColumn("c6", ColumnType.DOUBLE);

        testTable.primaryKey("c2", "c3", "c4");
        testTable.notNull("c2", "c3", "c4");
        testTable.colocationKey("c2", "c3");

        TestSchema testSchema = new TestSchema();
        testSchema.tables.add(testTable);
        testSchema.init(catalogManager);

        SqlSchemaManager sqlSchemaManager = newSchemaManager();
        SchemaPlus schemaPlus = sqlSchemaManager.schema(testSchema.name, testSchema.timestamp);
        IgniteSchema schema = unwrapSchema(schemaPlus);

        IgniteTable table = (IgniteTable) schema.getTable(testTable.name);
        assertNotNull(table);

        IgniteDistribution distribution = table.descriptor().distribution();
        // TODO Use the actual zone ID after implementing https://issues.apache.org/jira/browse/IGNITE-18426.
        int tableId = table.id();
        assertEquals(IgniteDistributions.affinity(List.of(1, 2), tableId, tableId), distribution);
        assertEquals(distribution, table.getStatistic().getDistribution());
    }

    /**
     * Hash index.
     */
    @Test
    public void testHashIndex() {
        TestTable testTable = new TestTable("TEST");

        testTable.addColumn("c1", ColumnType.INT32);
        testTable.addColumn("c2", ColumnType.INT32);
        testTable.addColumn("c3", ColumnType.INT32);

        TestIndex testIndex = new TestIndex("TEST_IDX");
        testIndex.table = testTable.name;
        testIndex.hashColumns = Arrays.asList("c1", "c2");

        TestSchema testSchema = new TestSchema();
        testSchema.tables.add(testTable);
        testSchema.indexes.add(testIndex);
        testSchema.init(catalogManager);

        SqlSchemaManager sqlSchemaManager = newSchemaManager();
        SchemaPlus schemaPlus = sqlSchemaManager.schema(testSchema.name, testSchema.timestamp);
        IgniteSchema schema = unwrapSchema(schemaPlus);

        IgniteTable table = (IgniteTable) schema.getTable(testTable.name);
        assertNotNull(table);

        IgniteIndex testIdx = table.indexes().get(testIndex.name);

        assertEquals(testIndex.name, testIdx.name());
        assertEquals(Type.HASH, testIdx.type());
        assertEquals(RelCollations.of(
                new RelFieldCollation(0, Direction.CLUSTERED, NullDirection.UNSPECIFIED),
                new RelFieldCollation(1, Direction.CLUSTERED, NullDirection.UNSPECIFIED)
        ), testIdx.collation());
    }

    /**
     * Sorted index.
     */
    @Test
    public void testSortedIndex() {
        TestTable testTable = new TestTable("TEST");

        testTable.addColumn("c1", ColumnType.INT32);
        testTable.addColumn("c2", ColumnType.INT32);
        testTable.addColumn("c3", ColumnType.INT32);
        testTable.addColumn("c4", ColumnType.INT32);
        testTable.addColumn("c5", ColumnType.INT32);

        TestIndex testIndex = new TestIndex("TEST_IDX");
        testIndex.table = testTable.name;
        testIndex.sortedColumns = Arrays.asList(
                Map.entry("c1", CatalogColumnCollation.ASC_NULLS_LAST),
                Map.entry("c2", CatalogColumnCollation.ASC_NULLS_FIRST),
                Map.entry("c3", CatalogColumnCollation.DESC_NULLS_LAST),
                Map.entry("c4", CatalogColumnCollation.DESC_NULLS_FIRST)
        );

        TestSchema testSchema = new TestSchema();
        testSchema.tables.add(testTable);
        testSchema.indexes.add(testIndex);
        testSchema.init(catalogManager);

        SqlSchemaManager sqlSchemaManager = newSchemaManager();
        SchemaPlus schemaPlus = sqlSchemaManager.schema(testSchema.name, testSchema.timestamp);
        IgniteSchema schema = unwrapSchema(schemaPlus);

        IgniteTable table = (IgniteTable) schema.getTable(testTable.name);
        assertNotNull(table);

        IgniteIndex testIdx = table.indexes().get(testIndex.name);

        assertEquals(testIndex.name, testIdx.name());
        assertEquals(Type.SORTED, testIdx.type());
        assertEquals(RelCollations.of(
                new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST),
                new RelFieldCollation(1, Direction.ASCENDING, NullDirection.FIRST),
                new RelFieldCollation(2, Direction.DESCENDING, NullDirection.LAST),
                new RelFieldCollation(3, Direction.DESCENDING, NullDirection.FIRST)
        ), testIdx.collation());
    }

    /**
     * Tests basic properties of a system view.
     */
    @ParameterizedTest
    @MethodSource("systemViewDistributions")
    public void testBasicView(SystemViewType viewType, IgniteDistribution distribution) {
        TestSystemView testSystemView = new TestSystemView("TEST", viewType);
        testSystemView.addColumn("c1", ColumnType.STRING);

        TestSchema testSchema = new TestSchema();
        testSchema.systemViews.add(testSystemView);
        testSchema.init(catalogManager);

        SqlSchemaManager sqlSchemaManager = newSchemaManager();
        SchemaPlus schemaPlus = sqlSchemaManager.schema(testSchema.name, testSchema.timestamp);
        IgniteSchema igniteSchema = unwrapSchema(schemaPlus);
        IgniteSystemView systemView = getSystemView(igniteSchema, testSystemView);

        assertEquals(testSystemView.id, systemView.id());
        assertEquals(distribution, systemView.distribution());
        assertEquals(distribution, systemView.getStatistic().getDistribution());
    }

    /**
     * Tests system view column types.
     */
    @ParameterizedTest
    @MethodSource("systemViewColumnTypes")
    public void testViewColumns(SystemViewType viewType, ColumnType columnType, int precision, int scale, boolean hasNativeType) {
        TestSystemView testSystemView = new TestSystemView("TEST", viewType);

        testSystemView.addColumn("c1_nullable", columnType, precision, scale);
        testSystemView.addColumn("c1_not_nullable", columnType, precision, scale);

        testSystemView.notNull("c1_not_nullable");

        TestSchema testSchema = new TestSchema();
        testSchema.systemViews.add(testSystemView);
        testSchema.init(catalogManager);

        SqlSchemaManager sqlSchemaManager = newSchemaManager();
        SchemaPlus schemaPlus = sqlSchemaManager.schema(testSchema.name, testSchema.timestamp);
        IgniteSchema igniteSchema = unwrapSchema(schemaPlus);
        IgniteSystemView systemView = getSystemView(igniteSchema, testSystemView);

        TableDescriptor descriptor = systemView.descriptor();
        assertEquals(testSystemView.columns.size(), descriptor.columnsCount(), "column count");
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
                Arguments.of(SystemViewType.LOCAL, IgniteDistributions.identity(0)),
                Arguments.of(SystemViewType.GLOBAL, IgniteDistributions.single())
        );
    }


    private static IgniteSystemView getSystemView(IgniteSchema schema, TestSystemView testSystemView) {
        Table systemViewTable = schema.getTable(testSystemView.name);
        assertNotNull(systemViewTable);

        IgniteSystemView systemView = assertInstanceOf(IgniteSystemView.class, systemViewTable);
        assertEquals(systemView.name(), testSystemView.name);

        return systemView;
    }

    private CatalogSqlSchemaManager newSchemaManager() {
        return new CatalogSqlSchemaManager(catalogManager, 200);
    }

    private IgniteSchema unwrapSchema(SchemaPlus schemaPlus) {
        IgniteSchema igniteSchema = schemaPlus.unwrap(IgniteSchema.class);
        assertNotNull(igniteSchema);
        return igniteSchema;
    }

    private static IgniteTable getTable(IgniteSchema schema, TestTable testTable) {
        IgniteTable table = (IgniteTable) schema.getTable(testTable.name);
        assertNotNull(table);
        return table;
    }

    private static Stream<Arguments> columnTypes() {
        return Stream.of(
                Arguments.of(ColumnType.BOOLEAN, -1, -1, true),
                Arguments.of(ColumnType.INT8, -1, -1, true),
                Arguments.of(ColumnType.INT16, -1, -1, true),
                Arguments.of(ColumnType.INT32, -1, -1, true),
                Arguments.of(ColumnType.INT64, -1, -1, true),
                Arguments.of(ColumnType.FLOAT, -1, -1, true),
                Arguments.of(ColumnType.DOUBLE, -1, -1, true),
                Arguments.of(ColumnType.DECIMAL, 4, -1, true),
                Arguments.of(ColumnType.DECIMAL, 4, 2, true),
                Arguments.of(ColumnType.NUMBER, 4, -1, true),
                Arguments.of(ColumnType.STRING, 40, -1, true),
                Arguments.of(ColumnType.BYTE_ARRAY, 40, -1, true),
                Arguments.of(ColumnType.DATE, -1, -1, true),
                Arguments.of(ColumnType.TIME, 2, -1, true),
                Arguments.of(ColumnType.DATETIME, 2, -1, true),
                Arguments.of(ColumnType.TIMESTAMP, 2, -1, true),
                Arguments.of(ColumnType.PERIOD, 2, -1, false),
                Arguments.of(ColumnType.DURATION, 2, -1, false),
                Arguments.of(ColumnType.UUID, 2, -1, true),
                Arguments.of(ColumnType.BITMASK, 2, -1, true)
        );
    }

    private static final class TestSchema {

        final String name;

        final Set<TestTable> tables = new LinkedHashSet<>();

        final Set<TestIndex> indexes = new LinkedHashSet<>();

        final Set<TestSystemView> systemViews = new LinkedHashSet<>();

        TestSchema() {
            this(DEFAULT_SCHEMA_NAME);
        }

        TestSchema(String name) {
            this.name = name;
        }

        long timestamp = System.nanoTime();

        int version = 1111;

        void init(CatalogManager catalogManager) {
            CatalogSchemaDescriptor schemaDescriptor = newSchemaDescriptor(version);
            when(catalogManager.activeCatalogVersion(timestamp)).thenReturn(version);
            when(catalogManager.schema(name != null ? name : DEFAULT_SCHEMA_NAME, version)).thenReturn(schemaDescriptor);
        }

        CatalogSchemaDescriptor newSchemaDescriptor(int version) {
            LinkedHashMap<String, CatalogTableDescriptor> tableDescriptors = new LinkedHashMap<>();

            for (TestTable testTable : tables) {
                testTable.version = version;

                CatalogTableDescriptor descriptor = testTable.newDescriptor();
                tableDescriptors.put(testTable.name, descriptor);
            }

            LinkedHashMap<String, CatalogIndexDescriptor> indexDescriptorMap = new LinkedHashMap<>();

            for (TestIndex testIndex : indexes) {
                CatalogTableDescriptor tableDescriptor = tableDescriptors.get(testIndex.table);
                testIndex.version = version;

                int tableId = tableDescriptor.id();
                String name = testIndex.name;

                indexDescriptorMap.put(name, testIndex.newDescriptor(tableId));
            }

            LinkedHashMap<String, CatalogSystemViewDescriptor> systemViewDescriptorMap = new LinkedHashMap<>();

            for (TestSystemView testSystemView : systemViews) {
                CatalogSystemViewDescriptor descriptor = testSystemView.newDescriptor(testSystemView.id);

                systemViewDescriptorMap.put(testSystemView.name, descriptor);
            }

            CatalogTableDescriptor[] tablesArray = tableDescriptors.values().toArray(new CatalogTableDescriptor[0]);
            CatalogIndexDescriptor[] indexesArray = indexDescriptorMap.values().toArray(new CatalogIndexDescriptor[0]);
            CatalogSystemViewDescriptor[] systemViewsArray = systemViewDescriptorMap.values().toArray(new CatalogSystemViewDescriptor[0]);

            return new CatalogSchemaDescriptor(
                    ID.incrementAndGet(),
                    name,
                    tablesArray,
                    indexesArray,
                    systemViewsArray,
                    INITIAL_CAUSALITY_TOKEN
            );
        }
    }

    private abstract static class TestDataSource {

        final List<CatalogTableColumnDescriptor> columns = new ArrayList<>();

        int id = ID.incrementAndGet();

        final String name;

        final Set<String> notNull = new HashSet<>();

        final Map<String, DefaultValue> defaultValueMap = new HashMap<>();

        TestDataSource(String name) {
            this.name = name;
        }

        void addColumn(String name, ColumnType columnType) {
            DefaultValue defaultValue = DefaultValue.constant(null);
            columns.add(new CatalogTableColumnDescriptor(name, columnType, true, 0, 0, 0, defaultValue));
            setDefault(name, defaultValue);
        }

        void addColumn(String name, ColumnType columnType, int precision, int scale) {
            DefaultValue defaultValue = DefaultValue.constant(null);
            columns.add(new CatalogTableColumnDescriptor(name, columnType, true, precision, scale, precision, defaultValue));
            setDefault(name, defaultValue);
        }

        void setDefault(String name, DefaultValue defaultValue) {
            defaultValueMap.put(name, defaultValue);
        }

        // Adds NOT NULL constraint, Set nullable = false for the given columns.
        void notNull(String... names) {
            notNull.clear();
            notNull.addAll(Arrays.asList(names));
        }
    }

    private static final class TestTable extends TestDataSource {

        private final int zoneId = ID.incrementAndGet();

        private List<String> primaryKey = Collections.emptyList();

        private List<String> colocationKey;

        private int version;

        private TestTable(String name) {
            super(name);
        }

        // Sets primary key columns
        void primaryKey(String... names) {
            primaryKey = Arrays.asList(names);
        }

        // Set colocation key columns
        void colocationKey(String... names) {
            colocationKey = Arrays.asList(names);
        }

        CatalogTableDescriptor newDescriptor() {
            List<CatalogTableColumnDescriptor> columnDescriptors = new ArrayList<>();

            for (CatalogTableColumnDescriptor col : columns) {
                String colName = col.name();
                DefaultValue defaultValue = defaultValueMap.get(colName);
                boolean nullable = !notNull.contains(colName);
                int precision = col.precision();
                int scale = col.scale();
                int length = col.length();

                CatalogTableColumnDescriptor newCol = new CatalogTableColumnDescriptor(colName, col.type(), nullable,
                        precision, scale, length, defaultValue);
                columnDescriptors.add(newCol);
            }

            return new CatalogTableDescriptor(
                    id,
                    -1,
                    name,
                    zoneId,
                    CatalogTableDescriptor.INITIAL_TABLE_VERSION,
                    columnDescriptors,
                    primaryKey,
                    colocationKey,
                    INITIAL_CAUSALITY_TOKEN
            );
        }
    }

    private static final class TestIndex {

        private final String name;

        private int id = ID.incrementAndGet();

        private int version;

        private List<String> hashColumns;

        private List<Map.Entry<String, CatalogColumnCollation>> sortedColumns;

        private String table;

        private TestIndex(String name) {
            this.name = name;
        }

        CatalogIndexDescriptor newDescriptor(int tableId) {
            if (hashColumns != null) {
                return new CatalogHashIndexDescriptor(id, name, tableId, false, hashColumns);
            } else if (sortedColumns != null) {
                List<CatalogIndexColumnDescriptor> indexColumns = sortedColumns.stream()
                        .map((e) -> new CatalogIndexColumnDescriptor(e.getKey(), e.getValue()))
                        .collect(Collectors.toList());

                return new CatalogSortedIndexDescriptor(id, name, tableId, false, indexColumns);
            } else {
                throw new IllegalStateException("Unable to create index");
            }
        }
    }

    private static class TestSystemView extends TestDataSource {

        final SystemViewType systemViewType;

        private TestSystemView(String name, SystemViewType systemViewType) {
            super(name);
            this.systemViewType = systemViewType;
        }

        CatalogSystemViewDescriptor newDescriptor(int id) {
            return new CatalogSystemViewDescriptor(id, name, List.copyOf(columns), systemViewType);
        }
    }
}
