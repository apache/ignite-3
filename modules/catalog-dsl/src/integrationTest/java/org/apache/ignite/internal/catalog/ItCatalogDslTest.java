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

package org.apache.ignite.internal.catalog;

import static org.apache.ignite.catalog.ColumnType.INTEGER;
import static org.apache.ignite.catalog.ColumnType.VARCHAR;
import static org.apache.ignite.catalog.definitions.ColumnDefinition.column;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.will;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.catalog.ColumnSorted;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.IgniteCatalog;
import org.apache.ignite.catalog.IndexType;
import org.apache.ignite.catalog.SortOrder;
import org.apache.ignite.catalog.annotations.Id;
import org.apache.ignite.catalog.definitions.ColumnDefinition;
import org.apache.ignite.catalog.definitions.IndexDefinition;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.catalog.definitions.ZoneDefinition;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.matcher.TableDefinitionMatcher;
import org.apache.ignite.internal.matcher.ZoneDefinitionMatcher;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("ThrowableNotThrown")
class ItCatalogDslTest extends ClusterPerClassIntegrationTest {

    static final String POJO_KV_TABLE_NAME = "POJO_KV_TEST";

    static final String POJO_RECORD_TABLE_NAME = "pojo_record_test";

    static final String POJO_RECORD_EXTENDED_TABLE_NAME = "pojo_record_extended_test";

    static final String EXPLICIT_QUOTES_TABLE_NAME = "explicit_quotes_test_table";

    static final String ZONE_NAME = "ZONE_TEST";

    private static final int KEY = 1;

    private static final PojoKey POJO_KEY = new PojoKey(KEY, String.valueOf(KEY));

    private static final PojoValue POJO_VALUE = new PojoValue("fname", "lname", UUID.randomUUID().toString());

    private static final Pojo POJO_RECORD = new Pojo(1, "1", "fname", "lname", UUID.randomUUID().toString());

    @AfterEach
    void tearDown() {
        dropAllTables();
        dropAllSchemas();

        sql("DROP ZONE IF EXISTS " + ZONE_NAME);
    }

    @Test
    void zoneCreateAndDropByDefinition() {
        // Given zone definition
        ZoneDefinition zoneDefinition = ZoneDefinition.builder(ZONE_NAME)
                .distributionAlgorithm("rendezvous")
                .filter(DEFAULT_FILTER)
                .storageProfiles(DEFAULT_AIPERSIST_PROFILE_NAME)
                .build();

        // When create zone from definition
        assertThat(catalog().createZoneAsync(zoneDefinition), willCompleteSuccessfully());

        // Then zone was created
        assertThrows(
                SqlException.class,
                () -> sql("CREATE ZONE " + ZONE_NAME + " STORAGE PROFILES ['" + DEFAULT_AIPERSIST_PROFILE_NAME + "']"),
                "Distribution zone with name '" + ZONE_NAME.toUpperCase() + "' already exists"
        );

        // When drop zone by definition
        assertThat(catalog().dropZoneAsync(zoneDefinition), willCompleteSuccessfully());

        // Then zone was dropped
        assertThrows(
                SqlException.class,
                () -> sql("DROP ZONE " + ZONE_NAME),
                "Distribution zone with name '" + ZONE_NAME.toUpperCase() + "' not found"
        );
    }

    @Test
    void zoneCreateAndDropByName() {
        // Given zone definition
        ZoneDefinition zoneDefinition = ZoneDefinition
                .builder(ZONE_NAME)
                .storageProfiles(DEFAULT_AIPERSIST_PROFILE_NAME)
                .build();

        // When create zone from definition
        assertThat(catalog().createZoneAsync(zoneDefinition), willCompleteSuccessfully());

        // Then zone was created
        assertThrows(
                SqlException.class,
                () -> sql("CREATE ZONE " + ZONE_NAME + " STORAGE PROFILES ['" + DEFAULT_AIPERSIST_PROFILE_NAME + "']"),
                "Distribution zone with name '" + ZONE_NAME.toUpperCase() + "' already exists"
        );

        // When drop zone by name
        assertThat(catalog().dropZoneAsync(ZONE_NAME), willCompleteSuccessfully());

        // Then zone was dropped
        assertThrows(
                SqlException.class,
                () -> sql("DROP ZONE " + ZONE_NAME),
                "Distribution zone with name '" + ZONE_NAME.toUpperCase() + "' not found"
        );
    }

    @Test
    void tableCreateAndDropByDefinition() {
        // Given table definition
        TableDefinition tableDefinition = TableDefinition.builder(POJO_KV_TABLE_NAME)
                .columns(column("id", ColumnType.INTEGER))
                .primaryKey("id")
                .build();

        // When create table from definition
        assertThat(catalog().createTableAsync(tableDefinition), will(not(nullValue())));

        // Then table was created
        assertThrows(
                SqlException.class,
                () -> sql("CREATE TABLE " + POJO_KV_TABLE_NAME + " (id int PRIMARY KEY)"),
                "Table with name " + toFullTableName(POJO_KV_TABLE_NAME) + " already exists"
        );

        // When drop table by definition
        assertThat(catalog().dropTableAsync(tableDefinition), willCompleteSuccessfully());

        // Then table is dropped
        assertThrows(
                SqlException.class,
                () -> sql("DROP TABLE " + POJO_KV_TABLE_NAME),
                "Table with name " + toFullTableName(POJO_KV_TABLE_NAME) + " not found"
        );
    }

    @Test
    void tableCreateAndDropByDefinitionWithExplicitQuotes() {
        // Given table definition
        TableDefinition tableDefinition = TableDefinition.builder(EXPLICIT_QUOTES_TABLE_NAME)
                .ifNotExists()
                .columns(
                        column("ID", INTEGER),
                        column("NAME", VARCHAR),
                        column("\"FooBar\"", ColumnType.varchar(20).notNull().defaultValue("a"))
                )
                .primaryKey("id")
                .index("name", "\"FooBar\"")
                .build();

        // When create table from definition
        assertThat(catalog().createTableAsync(tableDefinition), will(not(nullValue())));

        // Then table was created
        assertThrows(
                SqlException.class,
                () -> sql("CREATE TABLE " + EXPLICIT_QUOTES_TABLE_NAME + " (id int PRIMARY KEY)"),
                "Table with name " + toFullTableName(EXPLICIT_QUOTES_TABLE_NAME) + " already exists"
        );

        // When drop table by definition
        assertThat(catalog().dropTableAsync(tableDefinition), willCompleteSuccessfully());

        // Then table is dropped
        assertThrows(
                SqlException.class,
                () -> sql("DROP TABLE " + EXPLICIT_QUOTES_TABLE_NAME),
                "Table with name " + toFullTableName(EXPLICIT_QUOTES_TABLE_NAME) + " not found"
        );
    }

    @Test
    void tableCreateAndDropByName() {
        // Given table definition
        TableDefinition tableDefinition = TableDefinition.builder(POJO_KV_TABLE_NAME)
                .columns(column("id", ColumnType.INTEGER))
                .primaryKey("id")
                .build();

        // When create table from definition
        assertThat(catalog().createTableAsync(tableDefinition), will(not(nullValue())));

        // Then table was created
        assertThrows(
                SqlException.class,
                () -> sql("CREATE TABLE " + POJO_KV_TABLE_NAME + " (id int PRIMARY KEY)"),
                "Table with name " + toFullTableName(POJO_KV_TABLE_NAME) + " already exists"
        );

        // When drop table by name
        assertThat(catalog().dropTableAsync(POJO_KV_TABLE_NAME), willCompleteSuccessfully());

        // Then table is dropped
        assertThrows(
                SqlException.class,
                () -> sql("DROP TABLE " + POJO_KV_TABLE_NAME),
                "Table with name " + toFullTableName(POJO_KV_TABLE_NAME) + " not found"
        );
    }

    @Test
    void tableCreateAndDropWithQualifiedName() {
        createTable();

        QualifiedName name = QualifiedName.of("PUBLIC", POJO_KV_TABLE_NAME);

        // When drop table by qualified name with defined schema
        assertThat(catalog().dropTableAsync(name), willCompleteSuccessfully());

        createTable();

        name = QualifiedName.parse(POJO_KV_TABLE_NAME);

        // When drop table by qualified name without schema
        assertThat(catalog().dropTableAsync(name), willCompleteSuccessfully());

        String missedSchemaName = "MISSEDSCHEMA";

        QualifiedName nameWithMissedSchema = QualifiedName.of(missedSchemaName, POJO_KV_TABLE_NAME);

        // When drop table by qualified name with unknown schema then still completed successfully, because
        // CatalogDsl generates `DROP TABLE IF EXISTS` command
        assertThat(catalog().dropTableAsync(nameWithMissedSchema), willCompleteSuccessfully());
    }

    private static void createTable() {
        TableDefinition tableDefinition = TableDefinition.builder(POJO_KV_TABLE_NAME)
                .columns(column("id", ColumnType.INTEGER))
                .primaryKey("id")
                .build();

        catalog().createTable(tableDefinition);
    }

    private static String toFullTableName(String tableName) {
        return "'PUBLIC." + tableName.toUpperCase() + "'";
    }

    @Test
    void primitiveKeyKvViewFromAnnotation() throws Exception {
        CompletableFuture<Table> tableFuture = catalog().createTableAsync(Integer.class, PojoValue.class);
        assertThat(tableFuture, will(not(nullValue())));

        KeyValueView<Integer, PojoValue> keyValueView = tableFuture.get()
                .keyValueView(Integer.class, PojoValue.class);

        keyValueView.put(null, KEY, POJO_VALUE);
        assertThat(keyValueView.get(null, KEY), is(POJO_VALUE));
    }

    @Test
    void pojoKeyKvViewFromAnnotation() throws Exception {
        CompletableFuture<Table> tableFuture = catalog().createTableAsync(PojoKey.class, PojoValue.class);
        assertThat(tableFuture, will(not(nullValue())));

        KeyValueView<PojoKey, PojoValue> keyValueView = tableFuture.get()
                .keyValueView(PojoKey.class, PojoValue.class);

        keyValueView.put(null, POJO_KEY, POJO_VALUE);
        assertThat(keyValueView.get(null, POJO_KEY), is(POJO_VALUE));
    }

    @Test
    void primitiveKeyKvViewFromDefinition() throws Exception {
        TableDefinition definition = TableDefinition.builder(POJO_KV_TABLE_NAME)
                .key(Integer.class)
                .value(PojoValue.class)
                .build();

        CompletableFuture<Table> tableFuture = catalog().createTableAsync(definition);
        assertThat(tableFuture, will(not(nullValue())));

        KeyValueView<Integer, PojoValue> keyValueView = tableFuture.get().keyValueView(Integer.class, PojoValue.class);

        keyValueView.put(null, KEY, POJO_VALUE);
        assertThat(keyValueView.get(null, KEY), is(POJO_VALUE));
    }

    @Test
    void pojoKeyKvViewFromDefinition() throws Exception {
        TableDefinition definition = TableDefinition.builder(POJO_KV_TABLE_NAME)
                .key(PojoKey.class)
                .value(PojoValue.class)
                .build();

        CompletableFuture<Table> tableFuture = catalog().createTableAsync(definition);
        assertThat(tableFuture, will(not(nullValue())));

        KeyValueView<PojoKey, PojoValue> keyValueView = tableFuture.get().keyValueView(PojoKey.class, PojoValue.class);

        keyValueView.put(null, POJO_KEY, POJO_VALUE);
        assertThat(keyValueView.get(null, POJO_KEY), is(POJO_VALUE));
    }

    @Test
    void pojoRecordViewFromAnnotation() throws Exception {
        CompletableFuture<Table> tableFuture = catalog().createTableAsync(Pojo.class);
        assertThat(tableFuture, will(not(nullValue())));

        RecordView<Pojo> recordView = tableFuture.get().recordView(Pojo.class);

        assertThat(recordView.insert(null, POJO_RECORD), is(true));
        assertThat(recordView.get(null, POJO_RECORD), is(POJO_RECORD));
    }

    @Test
    void pojoRecordViewFromDefinition() throws Exception {
        TableDefinition definition = TableDefinition.builder(POJO_RECORD_TABLE_NAME).record(Pojo.class).build();

        CompletableFuture<Table> tableFuture = catalog().createTableAsync(definition);
        assertThat(tableFuture, will(not(nullValue())));

        RecordView<Pojo> recordView = tableFuture.get().recordView(Pojo.class);

        assertThat(recordView.insert(null, POJO_RECORD), is(true));
        assertThat(recordView.get(null, POJO_RECORD), is(POJO_RECORD));
    }

    @Test
    void createFromAnnotationAndInsertBySql() throws Exception {
        CompletableFuture<Table> tableFuture = catalog().createTableAsync(Pojo.class);
        assertThat(tableFuture, will(not(nullValue())));

        sql("insert into " + POJO_RECORD_TABLE_NAME + " (id, id_str, f_name, l_name, str) values (1, '1', 'f', 'l', 's')");
        List<List<Object>> rows = sql("select * from " + POJO_RECORD_TABLE_NAME);

        assertThat(rows, contains(List.of(1, "1", "f", "l", "s")));

        Pojo pojo = new Pojo(1, "1", "f", "l", "s");
        assertThat(tableFuture.get().recordView(Pojo.class).get(null, pojo), is(pojo));
    }

    @Test
    public void createAndGetDefinitionTest() {
        ZoneDefinition zoneDefinition = ZoneDefinition
                .builder(ZONE_NAME)
                .storageProfiles(DEFAULT_AIPERSIST_PROFILE_NAME)
                .partitions(3)
                .replicas(5)
                .quorumSize(2)
                .dataNodesAutoAdjustScaleDown(0)
                .dataNodesAutoAdjustScaleUp(1)
                .filter("$..*")
                .distributionAlgorithm("distributionAlgorithm")
                .consistencyMode(ConsistencyMode.HIGH_AVAILABILITY.name())
                .build();

        assertThat(catalog().createZoneAsync(zoneDefinition), willCompleteSuccessfully());

        ZoneDefinition actual = catalog().zoneDefinition(ZONE_NAME);
        assertThat(
                actual,
                ZoneDefinitionMatcher.isZoneDefinition()
                        .withZoneName(zoneDefinition.zoneName())
                        .withPartitions(zoneDefinition.partitions())
                        .withReplicas(zoneDefinition.replicas())
                        .withQuorumSize(zoneDefinition.quorumSize())
                        .withDataNodesAutoAdjustScaleDown(zoneDefinition.dataNodesAutoAdjustScaleDown())
                        .withDataNodesAutoAdjustScaleUp(zoneDefinition.dataNodesAutoAdjustScaleUp())
                        .withFilter(zoneDefinition.filter())
                        .withConsistencyMode(zoneDefinition.consistencyMode())
        // TODO: https://issues.apache.org/jira/browse/IGNITE-22162
        // .withDistributionAlgorithm(zoneDefinition.distributionAlgorithm())
        );

        ColumnDefinition column1 = column("COL1", ColumnType.INT32);
        ColumnDefinition column2 = column("COL2", ColumnType.INT64);
        ColumnDefinition column3 = column("COL3", ColumnType.BOOLEAN);
        ColumnDefinition column4 = column("COL4", ColumnType.VARCHAR);
        ColumnDefinition column5 = column("COL5", ColumnType.DECIMAL);

        TableDefinition definition = TableDefinition.builder(POJO_KV_TABLE_NAME)
                .zone(ZONE_NAME)
                .columns(List.of(column1, column2, column3, column4, column5))
                .primaryKey(IndexType.HASH, ColumnSorted.column(column1.name()), ColumnSorted.column(column3.name()))
                .index("INDEX_1", IndexType.HASH, ColumnSorted.column(column2.name()), ColumnSorted.column(column5.name()))
                .colocateBy(column3.name())
                .build();

        assertThat(catalog().createTableAsync(definition), willCompleteSuccessfully());

        assertThat(catalog().tableDefinitionAsync(POJO_KV_TABLE_NAME), willCompleteSuccessfully());

        List<Supplier<TableDefinition>> apiCallVariations = List.of(
                () -> catalog().tableDefinitionAsync(POJO_KV_TABLE_NAME).join(),
                () -> catalog().tableDefinition(POJO_KV_TABLE_NAME),
                () -> catalog().tableDefinition(QualifiedName.of("PUBLIC", POJO_KV_TABLE_NAME)),
                () -> catalog().tableDefinition(QualifiedName.parse(POJO_KV_TABLE_NAME))
        );

        for (Supplier<TableDefinition> supp : apiCallVariations) {
            assertThat(
                    supp.get(),
                    TableDefinitionMatcher.isTableDefinition()
                            .withTableName(definition.tableName())
                            .withZoneName(definition.zoneName())
                            .withColumns(definition.columns())
                            .withPkType(definition.primaryKeyType())
                            .withPkColumns(definition.primaryKeyColumns())
                            .withIndexes(definition.indexes())
                            .withColocationColumns(definition.colocationColumns())
            );
        }
    }

    @Test
    public void tableDefinitionWithIndexes() {
        sql("CREATE TABLE t (id int primary key, col1 varchar, col2 int, \"cOl3\" int)");
        sql("CREATE INDEX t_sorted ON t USING SORTED (col2 DESC, col1, \"cOl3\")");
        sql("CREATE INDEX t_hash ON t USING HASH (col1, col2)");

        sql("CREATE SCHEMA s");
        sql("CREATE TABLE s.t (id int primary key, col1 varchar, col2 int)");

        {
            TableDefinition table = catalog().tableDefinition(QualifiedName.of("PUBLIC", "T"));

            List<IndexDefinition> indexes = table.indexes();
            assertNotNull(indexes);

            Map<String, IndexDefinition> indexMap = indexes.stream()
                    .collect(Collectors.toMap(IndexDefinition::name, Function.identity()));

            assertEquals(Set.of("T_SORTED", "T_HASH"), indexMap.keySet());

            // primary index
            {
                assertEquals(IndexType.HASH, table.primaryKeyType());
                assertEquals(List.of(ColumnSorted.column("ID")), table.primaryKeyColumns());
            }
            // sorted index
            {
                IndexDefinition index = indexMap.get("T_SORTED");
                assertEquals(IndexType.SORTED, index.type());
                assertEquals(List.of(
                                ColumnSorted.column("COL2", SortOrder.DESC_NULLS_FIRST),
                                ColumnSorted.column("COL1", SortOrder.ASC_NULLS_LAST),
                                ColumnSorted.column("cOl3", SortOrder.ASC_NULLS_LAST)
                        ),
                        index.columns()
                );
            }
            // hash index
            {
                IndexDefinition index = indexMap.get("T_HASH");
                assertEquals(IndexType.HASH, index.type());
                assertEquals(List.of(ColumnSorted.column("COL1"), ColumnSorted.column("COL2")), index.columns());
            }
        }

        {
            TableDefinition table = catalog().tableDefinition(QualifiedName.of("S", "T"));

            List<IndexDefinition> indexes = table.indexes();
            assertNotNull(indexes);

            Map<String, IndexDefinition> indexMap = indexes.stream()
                    .collect(Collectors.toMap(IndexDefinition::name, Function.identity()));

            assertEquals(Set.of(), indexMap.keySet());

            // primary index
            {
                assertEquals(IndexType.HASH, table.primaryKeyType());
                assertEquals(List.of(ColumnSorted.column("ID")), table.primaryKeyColumns());
            }
        }
    }

    @Test
    public void tableDefinitionWithColocationColumn() {
        sql("CREATE TABLE t1 (id int, col1 varchar, col2 int, PRIMARY KEY (id))");
        {
            TableDefinition table = catalog().tableDefinition(QualifiedName.of("PUBLIC", "T1"));
            assertEquals(List.of("ID"), table.primaryKeyColumns().stream()
                    .map(ColumnSorted::columnName)
                    .collect(Collectors.toList())
            );
            assertEquals(List.of("ID"), table.colocationColumns());
        }

        sql("CREATE TABLE t2 (id int, col1 varchar, col2 int, PRIMARY KEY (col1, col2) ) COLOCATE BY (col1)");
        {
            TableDefinition table = catalog().tableDefinition(QualifiedName.of("PUBLIC", "T2"));
            assertEquals(List.of("COL1", "COL2"), table.primaryKeyColumns().stream()
                    .map(ColumnSorted::columnName)
                    .collect(Collectors.toList())
            );
            assertEquals(List.of("COL1"), table.colocationColumns());
        }

        sql("CREATE TABLE t3 (id int, col1 varchar, col2 int, PRIMARY KEY (col1, col2) ) COLOCATE BY (col2, col1)");
        {
            TableDefinition table = catalog().tableDefinition(QualifiedName.of("PUBLIC", "T3"));
            assertEquals(List.of("COL1", "COL2"), table.primaryKeyColumns().stream()
                    .map(ColumnSorted::columnName)
                    .collect(Collectors.toList())
            );
            assertEquals(List.of("COL2", "COL1"), table.colocationColumns());
        }

        sql("CREATE TABLE t4 (id int, col1 varchar, col2 int, PRIMARY KEY (col2, col1) ) COLOCATE BY (col1, col2)");
        {
            TableDefinition table = catalog().tableDefinition(QualifiedName.of("PUBLIC", "T4"));
            assertEquals(List.of("COL2", "COL1"), table.primaryKeyColumns().stream()
                    .map(ColumnSorted::columnName)
                    .collect(Collectors.toList())
            );
            assertEquals(List.of("COL1", "COL2"), table.colocationColumns());
        }
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    public void createAllColumnTypesFromPojo() {
        Table table = catalog().createTable(AllColumnTypesPojo.class);
        assertEquals("ALLCOLUMNTYPESPOJO", table.qualifiedName().objectName());

        TableDefinition tableDef = catalog().tableDefinition(table.qualifiedName());
        assertEquals(tableDef.tableName(), tableDef.tableName());

        List<ColumnDefinition> columns = tableDef.columns();
        assertEquals(15, columns.size());

        assertEquals("STR", columns.get(0).name());
        assertEquals("VARCHAR", columns.get(0).type().typeName());

        assertEquals("BYTECOL", columns.get(1).name());
        assertEquals("TINYINT", columns.get(1).type().typeName());

        assertEquals("SHORTCOL", columns.get(2).name());
        assertEquals("SMALLINT", columns.get(2).type().typeName());

        assertEquals("INTCOL", columns.get(3).name());
        assertEquals("INT", columns.get(3).type().typeName());

        assertEquals("LONGCOL", columns.get(4).name());
        assertEquals("BIGINT", columns.get(4).type().typeName());

        assertEquals("FLOATCOL", columns.get(5).name());
        assertEquals("REAL", columns.get(5).type().typeName());

        assertEquals("DOUBLECOL", columns.get(6).name());
        assertEquals("DOUBLE", columns.get(6).type().typeName());

        assertEquals("DECIMALCOL", columns.get(7).name());
        assertEquals("DECIMAL", columns.get(7).type().typeName());

        assertEquals("BOOLCOL", columns.get(8).name());
        assertEquals("BOOLEAN", columns.get(8).type().typeName());

        assertEquals("BYTESCOL", columns.get(9).name());
        assertEquals("VARBINARY", columns.get(9).type().typeName());

        assertEquals("UUIDCOL", columns.get(10).name());
        assertEquals("UUID", columns.get(10).type().typeName());

        assertEquals("DATECOL", columns.get(11).name());
        assertEquals("DATE", columns.get(11).type().typeName());

        assertEquals("TIMECOL", columns.get(12).name());
        assertEquals("TIME", columns.get(12).type().typeName());

        assertEquals("DATETIMECOL", columns.get(13).name());
        assertEquals("TIMESTAMP", columns.get(13).type().typeName());

        assertEquals("INSTANTCOL", columns.get(14).name());
        assertEquals("TIMESTAMP WITH LOCAL TIME ZONE", columns.get(14).type().typeName());
    }

    @Test
    public void testQuotedZoneName() {
        IgniteCatalog catalog = CLUSTER.node(0).catalog();

        sql("CREATE ZONE \"Some Zone\" STORAGE PROFILES ['" + DEFAULT_AIPERSIST_PROFILE_NAME + "']");

        ZoneDefinition zone = catalog.zoneDefinition("Some Zone");
        assertNotNull(zone);
        assertEquals("Some Zone", zone.zoneName());
    }

    @Test
    public void testQuotedTableName() {
        IgniteCatalog catalog = CLUSTER.node(0).catalog();

        sql("CREATE SCHEMA \"Table Schema\"");
        sql("CREATE TABLE \"Table Schema\".\"a b\" (id INT PRIMARY KEY, \"int val\" INT)");
        sql("CREATE INDEX \"a b index\" ON \"Table Schema\".\"a b\" (\"int val\")");

        QualifiedName name =  QualifiedName.parse("\"Table Schema\".\"a b\"");

        TableDefinition table = catalog.tableDefinition(name);
        assertNotNull(table);
        // Table Name
        assertEquals("\"a b\"", table.tableName());
        // Schema name
        assertEquals("\"Table Schema\"", table.schemaName());

        // Column
        List<ColumnDefinition> columns = table.columns();
        assertNotNull(columns);
        ColumnDefinition col = columns.stream()
                .filter(c -> "int val".equals(c.name()))
                .findAny()
                .orElse(null);
        assertNotNull(col, "Columns: " + columns.stream().map(ColumnDefinition::name).collect(Collectors.toList()));

        // Index
        List<IndexDefinition> indexes = table.indexes();
        assertNotNull(indexes);

        IndexDefinition index = indexes
                .stream().filter(idx -> Objects.equals(idx.name(), "a b index"))
                .findAny().orElse(null);
        assertNotNull(index, "Indexes: " + indexes.stream().map(IndexDefinition::name).collect(Collectors.toList()));

        catalog.dropTable(table);
    }

    @Test
    public void createDifferentSchemaFromDefinition() {
        sql("CREATE SCHEMA s");

        {
            TableDefinition def = TableDefinition.builder(POJO_KV_TABLE_NAME)
                    .columns(column("id", INTEGER), column("fname", VARCHAR), column("lname", VARCHAR))
                    .primaryKey("id")
                    .build();
            Table table = catalog().createTable(def);
            QualifiedName qualifiedName = QualifiedName.of("PUBLIC", POJO_KV_TABLE_NAME);
            assertEquals(qualifiedName, table.qualifiedName());

            TableDefinition tableDef = catalog().tableDefinition(def.qualifiedName());
            assertEquals(qualifiedName, tableDef.qualifiedName());
            assertEquals(POJO_KV_TABLE_NAME, tableDef.tableName());
            assertEquals("PUBLIC", tableDef.schemaName());
        }

        {
            TableDefinition def = TableDefinition.builder(POJO_KV_TABLE_NAME)
                    .schema("s")
                    .columns(column("id", INTEGER), column("fname", VARCHAR), column("lname", VARCHAR))
                    .primaryKey("id")
                    .build();

            Table table = catalog().createTable(def);
            QualifiedName qualifiedName = QualifiedName.of("S", POJO_KV_TABLE_NAME);
            assertEquals(qualifiedName, table.qualifiedName());

            TableDefinition tableDef = catalog().tableDefinition(def.qualifiedName());
            assertEquals(qualifiedName, tableDef.qualifiedName());
            assertEquals(POJO_KV_TABLE_NAME, tableDef.tableName());
            assertEquals("S", tableDef.schemaName());
        }

        // Quoted names
        sql("CREATE SCHEMA \"a Schema\"");

        {
            TableDefinition def = TableDefinition.builder("A_TABLE")
                    .schema("\"a Schema\"")
                    .columns(column("id", INTEGER), column("fname", VARCHAR), column("lname", VARCHAR))
                    .primaryKey("id")
                    .build();

            Table table = catalog().createTable(def);
            QualifiedName qualifiedName = QualifiedName.of("\"a Schema\"", "A_TABLE");
            assertEquals(qualifiedName, table.qualifiedName());

            TableDefinition tableDef = catalog().tableDefinition(def.qualifiedName());
            assertEquals(qualifiedName, tableDef.qualifiedName());
            assertEquals("A_TABLE", tableDef.tableName());
            assertEquals("\"a Schema\"", tableDef.schemaName());
        }

        {
            TableDefinition def = TableDefinition.builder("\"a tablE\"")
                    .schema("\"a Schema\"")
                    .columns(column("id", INTEGER), column("fname", VARCHAR), column("lname", VARCHAR))
                    .primaryKey("id")
                    .build();

            Table table = catalog().createTable(def);
            QualifiedName qualifiedName = QualifiedName.of("\"a Schema\"", "\"a tablE\"");
            assertEquals(qualifiedName, table.qualifiedName());

            TableDefinition tableDef = catalog().tableDefinition(def.qualifiedName());
            assertEquals(qualifiedName, tableDef.qualifiedName());
            assertEquals("\"a tablE\"", tableDef.tableName());
            assertEquals("\"a Schema\"", tableDef.schemaName());
        }
    }

    @Test
    public void createDifferentSchemaFromAnnotation() {
        sql("CREATE SCHEMA s");

        {
            Table table = catalog().createTable(PojoClass1.class);
            QualifiedName qualifiedName = QualifiedName.of("PUBLIC", POJO_KV_TABLE_NAME);
            assertEquals(qualifiedName, table.qualifiedName());

            TableDefinition def = catalog().tableDefinition(qualifiedName);
            assertEquals(qualifiedName, def.qualifiedName());
            assertEquals(POJO_KV_TABLE_NAME, def.tableName());
            assertEquals("PUBLIC", def.schemaName());
        }

        {
            Table table = catalog().createTable(PojoClass2.class);
            QualifiedName qualifiedName = QualifiedName.of("S", POJO_KV_TABLE_NAME);
            assertEquals(qualifiedName, table.qualifiedName());
            assertEquals(qualifiedName, catalog().tableDefinition(qualifiedName).qualifiedName());

            TableDefinition def = catalog().tableDefinition(qualifiedName);
            assertEquals(qualifiedName, def.qualifiedName());
            assertEquals(POJO_KV_TABLE_NAME, def.tableName());
            assertEquals("S", def.schemaName());
        }

        // Quoted names
        sql("CREATE SCHEMA \"a Schema\"");

        {
            Table table = catalog().createTable(PojoClass3.class);
            QualifiedName qualifiedName = QualifiedName.of("\"a Schema\"", "A_TABLE");
            assertEquals(qualifiedName, table.qualifiedName());

            TableDefinition tableDef = catalog().tableDefinition(qualifiedName);
            assertEquals(qualifiedName, tableDef.qualifiedName());
            assertEquals("A_TABLE", tableDef.tableName());
            assertEquals("\"a Schema\"", tableDef.schemaName());
        }

        {
            Table table = catalog().createTable(PojoClass4.class);
            QualifiedName qualifiedName = QualifiedName.of("\"a Schema\"", "\"a tablE\"");
            assertEquals(qualifiedName, table.qualifiedName());
            assertEquals(qualifiedName, catalog().tableDefinition(qualifiedName).qualifiedName());

            TableDefinition tableDef = catalog().tableDefinition(qualifiedName);
            assertEquals(qualifiedName, tableDef.qualifiedName());
            assertEquals("\"a tablE\"", tableDef.tableName());
            assertEquals("\"a Schema\"", tableDef.schemaName());
        }
    }

    @Test
    void inheritance() throws Exception {
        CompletableFuture<Table> tableFuture = catalog().createTableAsync(PojoExtended.class);
        assertThat(tableFuture, will(not(nullValue())));

        sql("insert into "
                + POJO_RECORD_EXTENDED_TABLE_NAME
                + " (id, id_str, f_name, l_name, str, f_name_extended) values (1, '1', 'f', 'l', 's', 'e')");
        List<List<Object>> rows = sql("select id, id_str, f_name, l_name, str, f_name_extended from "
                + POJO_RECORD_EXTENDED_TABLE_NAME);

        assertThat(rows, contains(List.of(1, "1", "f", "l", "s", "e")));

        PojoExtended pojo = new PojoExtended(1, "1", "f", "l", "s", "e");
        assertThat(tableFuture.get().recordView(PojoExtended.class).get(null, pojo), is(pojo));
    }

    private static IgniteCatalog catalog() {
        return CLUSTER.node(0).catalog();
    }

    @org.apache.ignite.catalog.annotations.Table(POJO_KV_TABLE_NAME)
    private static class PojoClass1 {
        @Id
        @SuppressWarnings("unused")
        Integer id;
        @SuppressWarnings("unused")
        String fname;
        @SuppressWarnings("unused")
        String lname;
    }

    @org.apache.ignite.catalog.annotations.Table(
            value = POJO_KV_TABLE_NAME,
            schemaName = "S"
    )
    private static class PojoClass2 {
        @Id
        @SuppressWarnings("unused")
        Integer id;
        @SuppressWarnings("unused")
        String fname;
        @SuppressWarnings("unused")
        String lname;
    }

    @org.apache.ignite.catalog.annotations.Table(
            value = "a_table",
            schemaName = "\"a Schema\""
    )
    private static class PojoClass3 {
        @Id
        @SuppressWarnings("unused")
        Integer id;
        @SuppressWarnings("unused")
        String fname;
        @SuppressWarnings("unused")
        String lname;
    }

    @org.apache.ignite.catalog.annotations.Table(
            value = "\"a tablE\"",
            schemaName = "\"a Schema\""
    )
    private static class PojoClass4 {
        @Id
        @SuppressWarnings("unused")
        Integer id;
        @SuppressWarnings("unused")
        String fname;
        @SuppressWarnings("unused")
        String lname;
    }
}
