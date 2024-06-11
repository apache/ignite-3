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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.IgniteCatalog;
import org.apache.ignite.catalog.SortOrder;
import org.apache.ignite.catalog.annotations.Column;
import org.apache.ignite.catalog.annotations.ColumnRef;
import org.apache.ignite.catalog.annotations.Id;
import org.apache.ignite.catalog.annotations.Index;
import org.apache.ignite.catalog.annotations.Table;
import org.apache.ignite.catalog.annotations.Zone;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.catalog.definitions.ZoneDefinition;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("ThrowableNotThrown")
class ItCatalogDslTest extends ClusterPerClassIntegrationTest {

    private static final String POJO_KV_TABLE_NAME = "pojo_kv_test";

    private static final String POJO_RECORD_TABLE_NAME = "pojo_record_test";

    private static final String ZONE_NAME = "zone_test";

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
                .affinity("affinity")
                .dataNodesAutoAdjust(1)
                .filter("filter")
                .storageProfiles(DEFAULT_AIPERSIST_PROFILE_NAME)
                .build();

        // When create zone from definition
        CompletableFuture<Void> zoneAsync = catalog().createZoneAsync(zoneDefinition);

        assertThat(zoneAsync, willCompleteSuccessfully());

        // Then zone was created
        assertThrows(
                SqlException.class,
                () -> sql("CREATE ZONE " + ZONE_NAME + " WITH STORAGE_PROFILES='" + DEFAULT_AIPERSIST_PROFILE_NAME + "'"),
                "Distribution zone with name '" + ZONE_NAME.toUpperCase() + "' already exists"
        );

        // When drop zone by definition
        CompletableFuture<Void> dropZone = catalog().dropZoneAsync(zoneDefinition);
        assertThat(dropZone, willCompleteSuccessfully());

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
        CompletableFuture<Void> createZone = catalog().createZoneAsync(zoneDefinition);
        assertThat(createZone, willCompleteSuccessfully());

        // Then zone was created
        assertThrows(
                SqlException.class,
                () -> sql("CREATE ZONE " + ZONE_NAME + " WITH STORAGE_PROFILES='" + DEFAULT_AIPERSIST_PROFILE_NAME + "'"),
                "Distribution zone with name '" + ZONE_NAME.toUpperCase() + "' already exists"
        );

        // When drop zone by name
        CompletableFuture<Void> dropZone = catalog().dropZoneAsync(ZONE_NAME);
        assertThat(dropZone, willCompleteSuccessfully());

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
        CompletableFuture<org.apache.ignite.table.Table> tableFuture = catalog().createTableAsync(tableDefinition);
        assertThat(tableFuture, will(not(nullValue())));

        // Then table was created
        assertThrows(
                SqlException.class,
                () -> sql("CREATE TABLE " + POJO_KV_TABLE_NAME + " (id int PRIMARY KEY)"),
                "Table with name " + toFullTableName(POJO_KV_TABLE_NAME) + " already exists"
        );

        // When drop table by definition
        catalog().dropTableAsync(tableDefinition);

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
        CompletableFuture<org.apache.ignite.table.Table> tableFuture = catalog().createTableAsync(tableDefinition);
        assertThat(tableFuture, will(not(nullValue())));

        // Then table was created
        assertThrows(
                SqlException.class,
                () -> sql("CREATE TABLE " + POJO_KV_TABLE_NAME + " (id int PRIMARY KEY)"),
                "Table with name " + toFullTableName(POJO_KV_TABLE_NAME) + " already exists"
        );

        // When drop table by name
        CompletableFuture<Void> dropTable = catalog().dropTableAsync(POJO_KV_TABLE_NAME);
        assertThat(dropTable, willCompleteSuccessfully());

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
        CompletableFuture<org.apache.ignite.table.Table> tableFuture = catalog().createTableAsync(Integer.class, PojoValue.class);
        assertThat(tableFuture, will(not(nullValue())));

        KeyValueView<Integer, PojoValue> keyValueView = tableFuture.get()
                .keyValueView(Integer.class, PojoValue.class);

        keyValueView.put(null, KEY, POJO_VALUE);
        assertThat(keyValueView.get(null, KEY), is(POJO_VALUE));
    }

    @Test
    void pojoKeyKvViewFromAnnotation() throws Exception {
        CompletableFuture<org.apache.ignite.table.Table> tableFuture = catalog().createTableAsync(PojoKey.class, PojoValue.class);
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

        CompletableFuture<org.apache.ignite.table.Table> tableFuture = catalog().createTableAsync(definition);
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

        CompletableFuture<org.apache.ignite.table.Table> tableFuture = catalog().createTableAsync(definition);
        assertThat(tableFuture, will(not(nullValue())));

        KeyValueView<PojoKey, PojoValue> keyValueView = tableFuture.get().keyValueView(PojoKey.class, PojoValue.class);

        keyValueView.put(null, POJO_KEY, POJO_VALUE);
        assertThat(keyValueView.get(null, POJO_KEY), is(POJO_VALUE));
    }

    @Test
    void pojoRecordViewFromAnnotation() throws Exception {
        CompletableFuture<org.apache.ignite.table.Table> tableFuture = catalog().createTableAsync(Pojo.class);
        assertThat(tableFuture, will(not(nullValue())));

        RecordView<Pojo> recordView = tableFuture.get().recordView(Pojo.class);

        assertThat(recordView.insert(null, POJO_RECORD), is(true));
        assertThat(recordView.get(null, POJO_RECORD), is(POJO_RECORD));
    }

    @Test
    void pojoRecordViewFromDefinition() throws Exception {
        TableDefinition definition = TableDefinition.builder(POJO_RECORD_TABLE_NAME).record(Pojo.class).build();

        CompletableFuture<org.apache.ignite.table.Table> tableFuture = catalog().createTableAsync(definition);
        assertThat(tableFuture, will(not(nullValue())));

        RecordView<Pojo> recordView = tableFuture.get().recordView(Pojo.class);

        assertThat(recordView.insert(null, POJO_RECORD), is(true));
        assertThat(recordView.get(null, POJO_RECORD), is(POJO_RECORD));
    }

    @Test
    void createFromAnnotationAndInsertBySql() throws Exception {
        CompletableFuture<org.apache.ignite.table.Table> tableFuture = catalog().createTableAsync(Pojo.class);
        assertThat(tableFuture, will(not(nullValue())));

        sql("insert into " + POJO_RECORD_TABLE_NAME + " (id, id_str, f_name, l_name, str) values (1, '1', 'f', 'l', 's')");
        List<List<Object>> rows = sql("select * from " + POJO_RECORD_TABLE_NAME);

        assertThat(rows, contains(List.of(1, "1", "f", "l", "s")));

        Pojo pojo = new Pojo(1, "1", "f", "l", "s");
        assertThat(tableFuture.get().recordView(Pojo.class).get(null, pojo), is(pojo));
    }

    private static IgniteCatalog catalog() {
        return CLUSTER.node(0).catalog();
    }

    private static IgniteTables tables() {
        return CLUSTER.node(0).tables();
    }

    private static class PojoKey {
        @Id
        Integer id;

        @Id
        @Column(value = "id_str", length = 20)
        String idStr;

        PojoKey() {}

        PojoKey(Integer id, String idStr) {
            this.id = id;
            this.idStr = idStr;
        }

    }

    @Table(
            value = POJO_KV_TABLE_NAME,
            zone = @Zone(
                    value = ZONE_NAME,
                    storageProfiles = DEFAULT_AIPERSIST_PROFILE_NAME
            ),
            colocateBy = @ColumnRef("id"),
            indexes = @Index(value = "ix_pojo", columns = {
                    @ColumnRef("f_name"),
                    @ColumnRef(value = "l_name", sort = SortOrder.DESC),
            })
    )
    private static class PojoValue {
        @Column("f_name")
        String firstName;

        @Column("l_name")
        String lastName;

        String str;

        PojoValue() {}

        PojoValue(String firstName, String lastName, String str) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.str = str;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PojoValue pojoValue = (PojoValue) o;
            return Objects.equals(firstName, pojoValue.firstName) && Objects.equals(lastName, pojoValue.lastName)
                    && Objects.equals(str, pojoValue.str);
        }

        @Override
        public int hashCode() {
            return Objects.hash(firstName, lastName, str);
        }
    }

    @Table(
            value = POJO_RECORD_TABLE_NAME,
            zone = @Zone(value = ZONE_NAME, storageProfiles = DEFAULT_AIPERSIST_PROFILE_NAME),
            colocateBy = @ColumnRef("id"),
            indexes = @Index(value = "ix_pojo", columns = {
                    @ColumnRef("f_name"),
                    @ColumnRef(value = "l_name", sort = SortOrder.DESC),
            })
    )
    private static class Pojo {
        @Id
        Integer id;

        @Id
        @Column(value = "id_str", length = 20)
        String idStr;

        @Column(value = "f_name", columnDefinition = "varchar(20) not null default 'a'")
        String firstName;

        @Column("l_name")
        String lastName;

        String str;

        Pojo() {}

        Pojo(Integer id, String idStr, String firstName, String lastName, String str) {
            this.id = id;
            this.idStr = idStr;
            this.firstName = firstName;
            this.lastName = lastName;
            this.str = str;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Pojo pojo = (Pojo) o;
            return Objects.equals(id, pojo.id) && Objects.equals(idStr, pojo.idStr) && Objects.equals(firstName,
                    pojo.firstName) && Objects.equals(lastName, pojo.lastName) && Objects.equals(str, pojo.str);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, idStr, firstName, lastName, str);
        }
    }
}
