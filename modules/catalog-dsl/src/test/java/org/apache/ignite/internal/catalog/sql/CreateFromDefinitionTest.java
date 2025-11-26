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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@SuppressWarnings("ThrowableNotThrown")
class CreateFromDefinitionTest {
    @Test
    void createFromZoneBuilderSimple() {
        ZoneDefinition zone = ZoneDefinition.builder("zone_test").storageProfiles("default").build();

        assertThat(createZone(zone), is("CREATE ZONE ZONE_TEST WITH STORAGE_PROFILES='default';"));
    }

    @Test
    void createFromZoneBuilder() {
        ZoneDefinition zone = ZoneDefinition.builder("zone_test")
                .ifNotExists()
                .partitions(3)
                .replicas(5)
                .quorumSize(2)
                .distributionAlgorithm("partitionDistribution")
                .dataNodesAutoAdjustScaleDown(2)
                .dataNodesAutoAdjustScaleUp(3)
                .filter("filter")
                .storageProfiles("default")
                .consistencyMode("HIGH_AVAILABILITY")
                .build();

        assertThat(
                createZone(zone),
                is("CREATE ZONE IF NOT EXISTS ZONE_TEST WITH STORAGE_PROFILES='default', PARTITIONS=3, REPLICAS=5, QUORUM_SIZE=2,"
                        + " DISTRIBUTION_ALGORITHM='partitionDistribution',"
                        + " DATA_NODES_AUTO_ADJUST_SCALE_UP=3, DATA_NODES_AUTO_ADJUST_SCALE_DOWN=2,"
                        + " DATA_NODES_FILTER='filter', CONSISTENCY_MODE='HIGH_AVAILABILITY';")
        );
    }

    @Test
    void testDefinitionInvalidConsistencyMode() {
        ZoneDefinition zoneDefinition = ZoneDefinition.builder("zone_test")
                .ifNotExists()
                .partitions(1)
                .replicas(3)
                .distributionAlgorithm("partitionDistribution")
                .dataNodesAutoAdjustScaleDown(2)
                .dataNodesAutoAdjustScaleUp(3)
                .filter("filter")
                .storageProfiles("default")
                .consistencyMode("MY_CONSISTENCY")
                .build();

        Assertions.assertThrows(IllegalArgumentException.class, () -> new CreateFromDefinitionImpl(null).from(zoneDefinition));
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

        assertThat(createTable(table), is("CREATE TABLE PUBLIC.BUILDER_TEST (ID INT);"));
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
                is("CREATE TABLE IF NOT EXISTS PUBLIC.BUILDER_TEST"
                        + " (ID INT, ID_STR VARCHAR, F_NAME VARCHAR(20) NOT NULL DEFAULT 'a', PRIMARY KEY (ID, ID_STR))"
                        + " COLOCATE BY (ID, ID_STR) ZONE ZONE_TEST;"
                        + System.lineSeparator()
                        + "CREATE INDEX IF NOT EXISTS IX_ID_STR_F_NAME ON PUBLIC.BUILDER_TEST (ID_STR, F_NAME);"
                        + System.lineSeparator()
                        + "CREATE INDEX IF NOT EXISTS IX_TEST ON PUBLIC.BUILDER_TEST USING SORTED (ID_STR ASC, F_NAME DESC NULLS LAST);")
        );
    }

    @Test
    void createFromTableBuilderQuoteNames() {
        TableDefinition table = TableDefinition.builder("\"builder test\"")
                .ifNotExists()
                .schema("\"sche ma\"")
                .colocateBy("id", "id str")
                .zone("zone test")
                .columns(
                        column("id", INTEGER),
                        column("id str", VARCHAR),
                        column("f name", ColumnType.varchar(20).notNull().defaultValue("a")),
                        column("\"LName\"", VARCHAR)
                )
                .primaryKey("id", "id str")
                .index("id str", "f name", "\"LName\"")
                .index("ix test", IndexType.SORTED, column("id str").asc(), column("f name").sort(DESC_NULLS_LAST))
                .build();

        assertThat(
                createTable(table),
                is("CREATE TABLE IF NOT EXISTS \"sche ma\".\"builder test\""
                        + " (ID INT, \"id str\" VARCHAR, \"f name\" VARCHAR(20) NOT NULL DEFAULT 'a', \"LName\" VARCHAR,"
                        + " PRIMARY KEY (ID, \"id str\")) COLOCATE BY (ID, \"id str\") ZONE \"zone test\";"
                        + System.lineSeparator()
                        + "CREATE INDEX IF NOT EXISTS \"ix_id str_f name_\"\"LName\"\"\" ON \"sche ma\".\"builder test\" (\"id str\","
                        + " \"f name\", \"LName\");"
                        + System.lineSeparator()
                        + "CREATE INDEX IF NOT EXISTS \"ix test\" ON \"sche ma\".\"builder test\" USING SORTED"
                        + " (\"id str\" ASC, \"f name\" DESC NULLS LAST);")
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
                is("CREATE TABLE PUBLIC.PRIMITIVE_TEST (ID INT, VAL INT, PRIMARY KEY (ID));")
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
                is("CREATE TABLE PUBLIC.POJO_VALUE_TEST (ID INT, F_NAME VARCHAR, L_NAME VARCHAR, STR VARCHAR, PRIMARY KEY (ID));")
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
                is("CREATE TABLE PUBLIC.POJO_VALUE_TEST"
                        + " (ID INT, ID_STR VARCHAR(20), F_NAME VARCHAR, L_NAME VARCHAR, STR VARCHAR, PRIMARY KEY (ID, ID_STR))"
                        + " COLOCATE BY (ID, ID_STR) ZONE ZONE_TEST;")
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
                is("CREATE TABLE IF NOT EXISTS PUBLIC.POJO_TEST (ID INT, ID_STR VARCHAR(20),"
                        + " F_NAME varchar(20) not null default 'a', L_NAME VARCHAR, STR VARCHAR,"
                        + " PRIMARY KEY (ID, ID_STR)) COLOCATE BY (ID, ID_STR) ZONE ZONE_TEST;")
        );
    }

    @Test
    void createFromRecordViewPrimitive() {
        TableDefinition tableDefinition = TableDefinition.builder("primitive_test")
                .record(Integer.class)
                .build();

        assertThat(
                createTable(tableDefinition),
                is("CREATE TABLE PUBLIC.PRIMITIVE_TEST (ID INT, PRIMARY KEY (ID));")
        );
    }

    @Test
    void createFromDefinitionDifferentCase() {
        String tableName = "Table";
        String quoted = String.format("\"%s\"", tableName);

        {
            TableDefinition definition = TableDefinition.builder(quoted)
                    .columns(column("id", INTEGER), column("col1", VARCHAR), column("col2", VARCHAR))
                    .primaryKey("id")
                    .build();

            assertThat(
                    createTable(definition),
                    is("CREATE TABLE PUBLIC.\"Table\" (ID INT, COL1 VARCHAR, COL2 VARCHAR, PRIMARY KEY (ID));")
            );
        }

        {
            TableDefinition definition = TableDefinition.builder(quoted)
                    .schema("\"Nice\"")
                    .columns(column("id", INTEGER), column("col1", VARCHAR), column("col2", VARCHAR))
                    .primaryKey("id")
                    .build();

            assertThat(
                    createTable(definition),
                    is("CREATE TABLE \"Nice\".\"Table\" (ID INT, COL1 VARCHAR, COL2 VARCHAR, PRIMARY KEY (ID));")
            );
        }
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
