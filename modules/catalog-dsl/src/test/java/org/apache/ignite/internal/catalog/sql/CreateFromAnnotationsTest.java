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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.ignite.catalog.IndexType;
import org.apache.ignite.catalog.SortOrder;
import org.apache.ignite.catalog.annotations.Column;
import org.apache.ignite.catalog.annotations.ColumnRef;
import org.apache.ignite.catalog.annotations.Id;
import org.apache.ignite.catalog.annotations.Index;
import org.apache.ignite.catalog.annotations.Table;
import org.apache.ignite.catalog.annotations.Zone;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.catalog.definitions.ZoneDefinition;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.mapper.PojoMapper;
import org.junit.jupiter.api.Test;

class CreateFromAnnotationsTest {

    @Test
    void testMapperCompatibility() {
        Mapper<Pojo> mapper = Mapper.of(Pojo.class);
        assertThat(mapper, instanceOf(PojoMapper.class));
        PojoMapper<Pojo> m = (PojoMapper<Pojo>) mapper;

        assertThat(m.targetType(), is(Pojo.class));
        assertThat(m.fields(), containsInAnyOrder("id", "idStr", "firstName", "lastName", "str"));

        // mapper columns in uppercase
        assertThat(m.fieldForColumn("ID"), is("id"));
        assertThat(m.fieldForColumn("ID_STR"), is("idStr"));
        assertThat(m.fieldForColumn("F_NAME"), is("firstName"));
        assertThat(m.fieldForColumn("L_NAME"), is("lastName"));
        assertThat(m.fieldForColumn("STR"), is("str"));
    }

    @Test
    void testDefinitionCompatibility() {
        ZoneDefinition zoneDefinition = ZoneDefinition.builder("zone_test")
                .ifNotExists()
                .partitions(1)
                .replicas(3)
                .affinity("affinity")
                .dataNodesAutoAdjust(1)
                .dataNodesAutoAdjustScaleDown(2)
                .dataNodesAutoAdjustScaleUp(3)
                .filter("filter")
                .storageProfiles("default")
                .build();
        Query query2 = new CreateFromDefinitionImpl(null).from(zoneDefinition);
        String sqlZoneFromDefinition = query2.toString();

        TableDefinition tableDefinition = TableDefinition.builder("pojo_value_test")
                .ifNotExists()
                .key(PojoKey.class)
                .value(PojoValue.class)
                .colocateBy("id", "id_str")
                .zone(zoneDefinition.zoneName())
                .index("ix_pojo", IndexType.DEFAULT, column("f_name"), column("l_name").desc())
                .build();
        CreateFromDefinitionImpl query1 = new CreateFromDefinitionImpl(null).from(tableDefinition);
        String sqlTableFromDefinition = query1.toString();

        CreateFromAnnotationsImpl query = createTable().processKeyValueClasses(PojoKey.class, PojoValue.class);
        String sqlFromAnnotations = query.toString();
        assertThat(sqlFromAnnotations, is(sqlZoneFromDefinition + System.lineSeparator() + sqlTableFromDefinition));
    }

    @Test
    void createFromKeyValueClassesPrimitive() {
        // primitive/boxed key class is a primary key with default name 'id'
        CreateFromAnnotationsImpl query = createTable().processKeyValueClasses(Integer.class, PojoValue.class);
        assertThat(
                query.toString(),
                is("CREATE ZONE IF NOT EXISTS zone_test WITH STORAGE_PROFILES='default', PARTITIONS=1, REPLICAS=3,"
                        + " AFFINITY_FUNCTION='affinity',"
                        + " DATA_NODES_AUTO_ADJUST=1, DATA_NODES_AUTO_ADJUST_SCALE_UP=3, DATA_NODES_AUTO_ADJUST_SCALE_DOWN=2,"
                        + " DATA_NODES_FILTER='filter';"
                        + System.lineSeparator()
                        + "CREATE TABLE IF NOT EXISTS pojo_value_test (id int, f_name varchar, l_name varchar, str varchar,"
                        + " PRIMARY KEY (id)) COLOCATE BY (id, id_str) WITH PRIMARY_ZONE='ZONE_TEST';"
                        + System.lineSeparator()
                        + "CREATE INDEX IF NOT EXISTS ix_pojo ON pojo_value_test (f_name, l_name desc);")
        );
    }

    @Test
    void createFromKeyValueClasses() {
        // key class fields (annotated only) is a composite primary keys
        CreateFromAnnotationsImpl query = createTable().processKeyValueClasses(PojoKey.class, PojoValue.class);
        assertThat(
                query.toString(),
                is("CREATE ZONE IF NOT EXISTS zone_test WITH STORAGE_PROFILES='default', PARTITIONS=1, REPLICAS=3,"
                        + " AFFINITY_FUNCTION='affinity',"
                        + " DATA_NODES_AUTO_ADJUST=1, DATA_NODES_AUTO_ADJUST_SCALE_UP=3, DATA_NODES_AUTO_ADJUST_SCALE_DOWN=2,"
                        + " DATA_NODES_FILTER='filter';"
                        + System.lineSeparator()
                        + "CREATE TABLE IF NOT EXISTS pojo_value_test (id int, id_str varchar(20), f_name varchar, l_name varchar,"
                        + " str varchar, PRIMARY KEY (id, id_str)) COLOCATE BY (id, id_str) WITH PRIMARY_ZONE='ZONE_TEST';"
                        + System.lineSeparator()
                        + "CREATE INDEX IF NOT EXISTS ix_pojo ON pojo_value_test (f_name, l_name desc);")
        );
    }

    @Test
    void createFromRecordClass() {
        CreateFromAnnotationsImpl query = createTable().processRecordClass(Pojo.class);
        assertThat(
                query.toString(),
                is("CREATE ZONE IF NOT EXISTS zone_test WITH STORAGE_PROFILES='default', PARTITIONS=1, REPLICAS=3,"
                        + " AFFINITY_FUNCTION='affinity',"
                        + " DATA_NODES_AUTO_ADJUST=1, DATA_NODES_AUTO_ADJUST_SCALE_UP=3, DATA_NODES_AUTO_ADJUST_SCALE_DOWN=2,"
                        + " DATA_NODES_FILTER='filter';"
                        + System.lineSeparator()
                        + "CREATE TABLE IF NOT EXISTS pojo_test (id int, id_str varchar(20), f_name varchar(20) not null default 'a',"
                        + " l_name varchar, str varchar, PRIMARY KEY (id, id_str))"
                        + " COLOCATE BY (id, id_str) WITH PRIMARY_ZONE='ZONE_TEST';"
                        + System.lineSeparator()
                        + "CREATE INDEX IF NOT EXISTS ix_pojo ON pojo_test (f_name, l_name desc);")
        );
    }

    @Test
    void nameGeneration() {
        CreateFromAnnotationsImpl query = createTable().processRecordClass(NameGeneration.class);
        assertThat(
                query.toString(),
                is("CREATE TABLE IF NOT EXISTS public.NameGeneration (col1 int, col2 varchar);"
                        + System.lineSeparator()
                        + "CREATE INDEX IF NOT EXISTS ix_col1_col2 ON public.NameGeneration (col1, col2);")
        );
    }

    @Test
    void primaryKey() {
        CreateFromAnnotationsImpl query = createTable().processRecordClass(PkSort.class);
        assertThat(
                query.toString(),
                is("CREATE TABLE IF NOT EXISTS PkSort (id int, PRIMARY KEY USING SORTED (id desc));")
        );
    }

    @Test
    void nativeTypes() {
        assertThrows(IllegalArgumentException.class, () -> createTable().processKeyValueClasses(Integer.class, Integer.class));
    }

    @Test
    void noAnnotations() {
        assertThrows(IllegalArgumentException.class, () -> createTable().processKeyValueClasses(NoAnnotations.class, NoAnnotations.class));
        assertThrows(IllegalArgumentException.class, () -> createTable().processRecordClass(NoAnnotations.class));
    }

    @Zone(
            value = "zone_test",
            partitions = 1,
            replicas = 3,
            affinityFunction = "affinity",
            dataNodesAutoAdjust = 1,
            dataNodesAutoAdjustScaleDown = 2,
            dataNodesAutoAdjustScaleUp = 3,
            filter = "filter",
            storageProfiles = "default"
    )
    private static class ZoneTest {}

    @SuppressWarnings("unused")
    private static class PojoKey {
        @Id
        Integer id;

        @Id
        @Column(value = "id_str", length = 20)
        String idStr;
    }

    @SuppressWarnings("unused")
    @Table(
            value = "pojo_value_test",
            zone = ZoneTest.class,
            colocateBy = {@ColumnRef("id"), @ColumnRef("id_str")},
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
    }

    @SuppressWarnings("unused")
    @Table(
            value = "pojo_test",
            zone = ZoneTest.class,
            colocateBy = {@ColumnRef("id"), @ColumnRef("id_str")},
            indexes = @Index(value = "ix_pojo", columns = {
                    @ColumnRef("f_name"),
                    @ColumnRef(value = "l_name", sort = SortOrder.DESC)
            })
    )
    static class Pojo {
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
    }

    @Table(
            schemaName = "public",
            indexes = @Index(columns = {@ColumnRef("col1"), @ColumnRef("col2")})
    )
    private static class NameGeneration {
        Integer col1;
        String col2;
    }

    @SuppressWarnings("unused")
    @Table(primaryKeyType = IndexType.SORTED)
    private static class PkSort {
        @Id(SortOrder.DESC)
        Integer id;
    }

    private static class NoAnnotations {
    }

    private static CreateFromAnnotationsImpl createTable() {
        return new CreateFromAnnotationsImpl(null);
    }
}
