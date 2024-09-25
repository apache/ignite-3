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

import static org.apache.ignite.catalog.definitions.ColumnDefinition.column;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.will;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.catalog.ColumnSorted;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.IgniteCatalog;
import org.apache.ignite.catalog.IndexType;
import org.apache.ignite.catalog.definitions.ColumnDefinition;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.catalog.definitions.ZoneDefinition;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.matcher.TableDefinitionMatcher;
import org.apache.ignite.internal.matcher.ZoneDefinitionMatcher;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("ThrowableNotThrown")
class ItCatalogDslTest extends ClusterPerClassIntegrationTest {

    static final String POJO_KV_TABLE_NAME = "POJO_KV_TEST";

    static final String POJO_RECORD_TABLE_NAME = "pojo_record_test";

    static final String ZONE_NAME = "ZONE_TEST";

    private static final int KEY = 1;

    private static final PojoKey POJO_KEY = new PojoKey(KEY, String.valueOf(KEY));

    private static final PojoValue POJO_VALUE = new PojoValue("fname", "lname", UUID.randomUUID().toString());

    private static final Pojo POJO_RECORD = new Pojo(1, "1", "fname", "lname", UUID.randomUUID().toString());

    @AfterEach
    void tearDown() {
        sql("DROP TABLE IF EXISTS " + POJO_KV_TABLE_NAME);
        sql("DROP TABLE IF EXISTS " + POJO_RECORD_TABLE_NAME);
        sql("DROP ZONE IF EXISTS " + ZONE_NAME);
    }

    @Test
    void zoneCreateAndDropByDefinition() {
        // Given zone definition
        ZoneDefinition zoneDefinition = ZoneDefinition.builder(ZONE_NAME)
                .distributionAlgorithm("rendezvous")
                .dataNodesAutoAdjust(1)
                .filter("filter")
                .storageProfiles(DEFAULT_AIPERSIST_PROFILE_NAME)
                .build();

        // When create zone from definition
        assertThat(catalog().createZoneAsync(zoneDefinition), willCompleteSuccessfully());

        // Then zone was created
        assertThrows(
                SqlException.class,
                () -> sql("CREATE ZONE " + ZONE_NAME + " WITH STORAGE_PROFILES='" + DEFAULT_AIPERSIST_PROFILE_NAME + "'"),
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
                () -> sql("CREATE ZONE " + ZONE_NAME + " WITH STORAGE_PROFILES='" + DEFAULT_AIPERSIST_PROFILE_NAME + "'"),
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
                .replicas(3)
                .dataNodesAutoAdjustScaleDown(0)
                .dataNodesAutoAdjustScaleUp(1)
                .filter("$..*")
                .distributionAlgorithm("distributionAlgorithm")
                .build();

        assertThat(catalog().createZoneAsync(zoneDefinition), willCompleteSuccessfully());

        ZoneDefinition actual = catalog().zoneDefinition(ZONE_NAME);
        assertThat(
                actual,
                ZoneDefinitionMatcher.isZoneDefinition()
                        .withZoneName(zoneDefinition.zoneName())
                        .withPartitions(zoneDefinition.partitions())
                        .withReplicas(zoneDefinition.replicas())
                        .withDataNodesAutoAdjustScaleDown(zoneDefinition.dataNodesAutoAdjustScaleDown())
                        .withDataNodesAutoAdjustScaleUp(zoneDefinition.dataNodesAutoAdjustScaleUp())
                        .withFilter(zoneDefinition.filter())
                        // TODO: https://issues.apache.org/jira/browse/IGNITE-22162
//                        .withDistributionAlgorithm(zoneDefinition.distributionAlgorithm())
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

        TableDefinition actual1 = catalog().tableDefinition(POJO_KV_TABLE_NAME);
        assertThat(
                actual1,
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

    private static IgniteCatalog catalog() {
        return CLUSTER.node(0).catalog();
    }
}
