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

package org.apache.ignite.internal.catalog.sql;

import static org.apache.ignite.catalog.ColumnSorted.column;
import static org.apache.ignite.catalog.ColumnType.INTEGER;
import static org.apache.ignite.catalog.ColumnType.VARCHAR;
import static org.apache.ignite.catalog.SortOrder.DESC_NULLS_LAST;
import static org.apache.ignite.catalog.definitions.ColumnDefinition.column;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.IndexType;
import org.apache.ignite.catalog.annotations.Column;
import org.apache.ignite.catalog.annotations.Id;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.catalog.definitions.ZoneDefinition;
import org.apache.ignite.internal.catalog.sql.CreateFromAnnotationsTest.Pojo;
import org.junit.jupiter.api.Test;

@SuppressWarnings("ThrowableNotThrown")
class CreateFromDefinitionTest {
    @Test
    void createFromZoneBuilderSimple() {
        ZoneDefinition zone = ZoneDefinition.builder("zone_test").storageProfiles("default").build();

        assertThat(createZone(zone), is("CREATE ZONE zone_test WITH STORAGE_PROFILES='default';"));
    }

    @Test
    void createFromZoneBuilder() {
        ZoneDefinition zone = ZoneDefinition.builder("zone_test")
                .ifNotExists()
                .partitions(3)
                .replicas(3)
                .affinity("affinity")
                .dataNodesAutoAdjust(1)
                .dataNodesAutoAdjustScaleDown(2)
                .dataNodesAutoAdjustScaleUp(3)
                .filter("filter")
                .storageProfiles("default")
                .build();

        assertThat(
                createZone(zone),
                is("CREATE ZONE IF NOT EXISTS zone_test WITH STORAGE_PROFILES='default', PARTITIONS=3, REPLICAS=3,"
                        + " AFFINITY_FUNCTION='affinity',"
                        + " DATA_NODES_AUTO_ADJUST=1, DATA_NODES_AUTO_ADJUST_SCALE_UP=3, DATA_NODES_AUTO_ADJUST_SCALE_DOWN=2,"
                        + " DATA_NODES_FILTER='filter';")
        );
    }

    @Test
    void invalidTableDefinition() {
        assertThrows(IllegalArgumentException.class,
                () -> createTable(TableDefinition.builder("empty").build()),
                "Columns list must not be empty.");
    }

    @Test
    void createFromTableBuilderSimple() {
        TableDefinition table = TableDefinition.builder("builder_test")
                .columns(column("id", INTEGER))
                .build();

        assertThat(createTable(table), is("CREATE TABLE builder_test (id int);"));
    }

    @Test
    void createFromTableBuilder() {
        TableDefinition table = TableDefinition.builder("builder_test")
                .ifNotExists()
                .colocateBy("id", "id_str")
                .zone("zone_test")
                .columns(
                        column("id", INTEGER),
                        column("id_str", VARCHAR),
                        column("f_name", ColumnType.varchar(20).notNull().defaultValue("a"))
                )
                .primaryKey("id", "id_str")
                .index("id_str", "f_name")
                .index("ix_test", IndexType.SORTED, column("id_str").asc(), column("f_name").sort(DESC_NULLS_LAST))
                .build();

        assertThat(
                createTable(table),
                is("CREATE TABLE IF NOT EXISTS builder_test"
                        + " (id int, id_str varchar, f_name varchar(20) NOT NULL DEFAULT 'a', PRIMARY KEY (id, id_str))"
                        + " COLOCATE BY (id, id_str) WITH PRIMARY_ZONE='ZONE_TEST';"
                        + "CREATE INDEX IF NOT EXISTS ix_id_str_f_name ON builder_test (id_str, f_name);"
                        + "CREATE INDEX IF NOT EXISTS ix_test ON builder_test USING SORTED (id_str asc, f_name desc nulls last);")
        );
    }

    @Test
    void createFromKeyValueViewPrimitiveKeyAndValue() {
        // primitive/boxed key class is a primary key with default name 'id'
        TableDefinition tableDefinition = TableDefinition.builder("primitive_test")
                .key(Integer.class)
                .value(Integer.class)
                .build();

        assertThat(
                createTable(tableDefinition),
                is("CREATE TABLE primitive_test (id int, val int, PRIMARY KEY (id));")
        );
    }

    @Test
    void createFromKeyValueViewPrimitiveKeyAnnotatedValue() {
        // primitive/boxed key class is a primary key with default name 'id'
        TableDefinition tableDefinition = TableDefinition.builder("pojo_value_test")
                .key(Integer.class)
                .value(PojoValue.class)
                .build();

        assertThat(
                createTable(tableDefinition),
                is("CREATE TABLE pojo_value_test (id int, f_name varchar, l_name varchar, str varchar, PRIMARY KEY (id));")
        );
    }

    @Test
    void createFromKeyValueViewAnnotatedKeyAndValue() {
        // key class fields (annotated only) is a composite primary keys
        TableDefinition tableDefinition = TableDefinition.builder("pojo_value_test")
                .colocateBy("id", "id_str")
                .zone("zone_test")
                .key(PojoKey.class)
                .value(PojoValue.class)
                .build();

        assertThat(
                createTable(tableDefinition),
                is("CREATE TABLE pojo_value_test"
                        + " (id int, id_str varchar(20), f_name varchar, l_name varchar, str varchar, PRIMARY KEY (id, id_str))"
                        + " COLOCATE BY (id, id_str) WITH PRIMARY_ZONE='ZONE_TEST';")
        );
    }

    @Test
    void createFromRecordView() {
        TableDefinition tableDefinition = TableDefinition.builder("pojo_test")
                .ifNotExists()
                .colocateBy("id", "id_str")
                .zone("zone_test")
                .record(Pojo.class)
                .build();

        assertThat(
                createTable(tableDefinition),
                is("CREATE TABLE IF NOT EXISTS pojo_test (id int, id_str varchar(20),"
                        + " f_name varchar(20) not null default 'a', l_name varchar, str varchar,"
                        + " PRIMARY KEY (id, id_str)) COLOCATE BY (id, id_str) WITH PRIMARY_ZONE='ZONE_TEST';")
        );
    }

    @Test
    void createFromRecordViewPrimitive() {
        TableDefinition tableDefinition = TableDefinition.builder("primitive_test")
                .record(Integer.class)
                .build();

        assertThat(
                createTable(tableDefinition),
                is("CREATE TABLE primitive_test (id int, PRIMARY KEY (id));")
        );
    }

    @SuppressWarnings("unused")
    private static class PojoKey {
        @Id
        Integer id;

        @Id
        @Column(value = "id_str", length = 20)
        String idStr;
    }

    @SuppressWarnings("unused")
    private static class PojoValue {
        @Column("f_name")
        String firstName;

        @Column("l_name")
        String lastName;

        String str;
    }

    private static String createZone(ZoneDefinition zoneDefinition) {
        return createTable().from(zoneDefinition).toString();
    }

    private static String createTable(TableDefinition tableDefinition) {
        return createTable().from(tableDefinition).toString();
    }

    private static CreateFromDefinitionImpl createTable() {
        return new CreateFromDefinitionImpl(null);
    }
}
