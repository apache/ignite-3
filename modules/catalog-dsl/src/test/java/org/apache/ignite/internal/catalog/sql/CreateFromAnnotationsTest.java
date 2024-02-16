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

import static org.apache.ignite.catalog.definitions.ColumnSorted.column;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.ignite.catalog.IndexType;
import org.apache.ignite.catalog.Options;
import org.apache.ignite.catalog.SortOrder;
import org.apache.ignite.catalog.ZoneEngine;
import org.apache.ignite.catalog.annotations.Col;
import org.apache.ignite.catalog.annotations.Column;
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
    private static final Options DEFAULT_OPTIONS = Options.defaultOptions();

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
                .engine(ZoneEngine.AIMEM)
                .partitions(1)
                .replicas(3)
                .build();
        String sqlZoneFromDefinition = new CreateFromDefinitionImpl(null, DEFAULT_OPTIONS).from(zoneDefinition).toSqlString();

        TableDefinition tableDefinition = TableDefinition.builder("pojo_value_test")
                .ifNotExists()
                .keyValueView(PojoKey.class, PojoValue.class)
                .colocateBy("id", "id_str")
                .zone(zoneDefinition.zoneName())
                .index("ix_pojo", IndexType.DEFAULT, column("f_name"), column("l_name").desc())
                .build();
        String sqlTableFromDefinition = new CreateFromDefinitionImpl(null, DEFAULT_OPTIONS).from(tableDefinition).toSqlString();

        String sqlFromAnnotations = createTable().keyValueView(PojoKey.class, PojoValue.class).toSqlString();
        assertThat(sqlFromAnnotations, is(sqlZoneFromDefinition + sqlTableFromDefinition));
    }

    @Test
    void createFromKeyValueViewPrimitive() {
        // primitive/boxed key class is a primary key with default name 'id'
        assertThat(
                createTable().keyValueView(Integer.class, PojoValue.class).toSqlString(),
                is("CREATE ZONE IF NOT EXISTS zone_test ENGINE AIMEM WITH PARTITIONS=1, REPLICAS=3;"
                        + "CREATE TABLE IF NOT EXISTS pojo_value_test (id int, f_name varchar, l_name varchar, str varchar,"
                        + " PRIMARY KEY (id)) COLOCATE BY (id, id_str) WITH PRIMARY_ZONE='ZONE_TEST';"
                        + "CREATE INDEX IF NOT EXISTS ix_pojo ON pojo_value_test (f_name, l_name desc);")
        );
    }

    @Test
    void createFromKeyValueViewPrimitiveQuoted() {
        // primitive/boxed key class is a primary key with default name 'id'
        assertThat(
                createTableQuoted().keyValueView(Integer.class, PojoValue.class).toSqlString(),
                is("CREATE ZONE IF NOT EXISTS \"zone_test\" ENGINE AIMEM WITH PARTITIONS=1, REPLICAS=3;"
                        + "CREATE TABLE IF NOT EXISTS \"pojo_value_test\" (\"id\" int, \"f_name\" varchar, \"l_name\" varchar,"
                        + " \"str\" varchar, PRIMARY KEY (\"id\")) COLOCATE BY (\"id\", \"id_str\") WITH PRIMARY_ZONE='ZONE_TEST';"
                        + "CREATE INDEX IF NOT EXISTS \"ix_pojo\" ON \"pojo_value_test\" (\"f_name\", \"l_name\" desc);")
        );
    }

    @Test
    void createFromKeyValueViewClass() {
        // key class fields (annotated only) is a composite primary keys
        assertThat(
                createTable().keyValueView(PojoKey.class, PojoValue.class).toSqlString(),
                is("CREATE ZONE IF NOT EXISTS zone_test ENGINE AIMEM WITH PARTITIONS=1, REPLICAS=3;"
                        + "CREATE TABLE IF NOT EXISTS pojo_value_test (id int, id_str varchar(20), f_name varchar, l_name varchar,"
                        + " str varchar, PRIMARY KEY (id, id_str)) COLOCATE BY (id, id_str) WITH PRIMARY_ZONE='ZONE_TEST';"
                        + "CREATE INDEX IF NOT EXISTS ix_pojo ON pojo_value_test (f_name, l_name desc);")
        );
    }

    @Test
    void createFromKeyValueViewClassQuoted() {
        // key class fields (annotated only) is a composite primary keys
        assertThat(
                createTableQuoted().keyValueView(PojoKey.class, PojoValue.class).toSqlString(),
                is("CREATE ZONE IF NOT EXISTS \"zone_test\" ENGINE AIMEM WITH PARTITIONS=1, REPLICAS=3;"
                        + "CREATE TABLE IF NOT EXISTS \"pojo_value_test\" (\"id\" int, \"id_str\" varchar(20), \"f_name\" varchar,"
                        + " \"l_name\" varchar, \"str\" varchar, PRIMARY KEY (\"id\", \"id_str\")) COLOCATE BY (\"id\", \"id_str\")"
                        + " WITH PRIMARY_ZONE='ZONE_TEST';"
                        + "CREATE INDEX IF NOT EXISTS \"ix_pojo\" ON \"pojo_value_test\" (\"f_name\", \"l_name\" desc);")
        );
    }

    @Test
    void createFromRecordView() {
        assertThat(
                createTable().recordView(Pojo.class).toSqlString(),
                is("CREATE ZONE IF NOT EXISTS zone_test ENGINE AIMEM WITH PARTITIONS=1, REPLICAS=3;"
                        + "CREATE TABLE IF NOT EXISTS pojo_test (id int, id_str varchar(20), f_name varchar(20) not null default 'a',"
                        + " l_name varchar, str varchar, PRIMARY KEY (id, id_str))"
                        + " COLOCATE BY (id, id_str) WITH PRIMARY_ZONE='ZONE_TEST';"
                        + "CREATE INDEX IF NOT EXISTS ix_pojo ON pojo_test (f_name, l_name desc);")
        );
    }

    @Test
    void createFromRecordViewQuoted() {
        assertThat(
                createTableQuoted().recordView(Pojo.class).toSqlString(),
                is("CREATE ZONE IF NOT EXISTS \"zone_test\" ENGINE AIMEM WITH PARTITIONS=1, REPLICAS=3;"
                        + "CREATE TABLE IF NOT EXISTS \"pojo_test\" (\"id\" int, \"id_str\" varchar(20),"
                        + " \"f_name\" varchar(20) not null default 'a', \"l_name\" varchar, \"str\" varchar,"
                        + " PRIMARY KEY (\"id\", \"id_str\")) COLOCATE BY (\"id\", \"id_str\") WITH PRIMARY_ZONE='ZONE_TEST';"
                        + "CREATE INDEX IF NOT EXISTS \"ix_pojo\" ON \"pojo_test\" (\"f_name\", \"l_name\" desc);")
        );
    }

    @Test
    void nameGeneration() {
        assertThat(
                createTable().recordView(NameGeneration.class).toSqlString(),
                is("CREATE TABLE IF NOT EXISTS NameGeneration (col1 int, col2 varchar);"
                        + "CREATE INDEX IF NOT EXISTS ix_col1_col2 ON NameGeneration (col1, col2);")
        );
    }

    @Test
    void nameGenerationQuoted() {
        assertThat(
                createTableQuoted().recordView(NameGeneration.class).toSqlString(),
                is("CREATE TABLE IF NOT EXISTS \"NameGeneration\" (\"col1\" int, \"col2\" varchar);"
                        + "CREATE INDEX IF NOT EXISTS \"ix_col1_col2\" ON \"NameGeneration\" (\"col1\", \"col2\");")
        );
    }

    @Test
    void primaryKey() {
        assertThat(
                createTable().recordView(PkSort.class).toSqlString(),
                is("CREATE TABLE IF NOT EXISTS PkSort (id int, PRIMARY KEY USING TREE (id desc));")
        );
    }

    @Test
    void primaryKeyQuoted() {
        assertThat(
                createTableQuoted().recordView(PkSort.class).toSqlString(),
                is("CREATE TABLE IF NOT EXISTS \"PkSort\" (\"id\" int, PRIMARY KEY USING TREE (\"id\" desc));")
        );
    }

    @Test
    void nativeTypes() {
        assertThrows(IllegalArgumentException.class, () -> createTable().keyValueView(Integer.class, Integer.class));
    }

    @Test
    void noAnnotations() {
        assertThrows(IllegalArgumentException.class, () -> createTable().keyValueView(NoAnnotations.class, NoAnnotations.class));
        assertThrows(IllegalArgumentException.class, () -> createTable().recordView(NoAnnotations.class));
    }

    @Zone(
            value = "zone_test",
            replicas = 3,
            partitions = 1,
            engine = ZoneEngine.AIMEM
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
            colocateBy = {@Col("id"), @Col("id_str")},
            indexes = @Index(value = "ix_pojo", columns = {
                    @Col("f_name"),
                    @Col(value = "l_name", sort = SortOrder.DESC),
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
            colocateBy = {@Col("id"), @Col("id_str")},
            indexes = @Index(value = "ix_pojo", columns = {
                    @Col("f_name"),
                    @Col(value = "l_name", sort = SortOrder.DESC),
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

    @Table(indexes = @Index(columns = {@Col("col1"), @Col("col2")}))
    private static class NameGeneration {
        Integer col1;
        String col2;
    }

    @SuppressWarnings("unused")
    @Table(primaryKeyType = IndexType.TREE)
    private static class PkSort {
        @Id(SortOrder.DESC)
        Integer id;
    }

    private static class NoAnnotations {
    }

    private static CreateFromAnnotationsImpl createTable() {
        return createTable(DEFAULT_OPTIONS);
    }

    private static CreateFromAnnotationsImpl createTable(Options options) {
        return new CreateFromAnnotationsImpl(null, options);
    }

    private static CreateFromAnnotationsImpl createTableQuoted() {
        return createTable(Options.defaultOptions().quoteIdentifiers());
    }
}
