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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
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

        assertNotNull(m.declaredFieldForColumn("ID"));
        assertNotNull(m.declaredFieldForColumn("ID_STR"));
        assertNotNull(m.declaredFieldForColumn("F_NAME"));
        assertNotNull(m.declaredFieldForColumn("L_NAME"));
        assertNotNull(m.declaredFieldForColumn("STR"));
    }

    @Test
    void testDefinitionCompatibility() {
        ZoneDefinition zoneDefinition = ZoneDefinition.builder("zone_test")
                .ifNotExists()
                .partitions(1)
                .replicas(5)
                .quorumSize(2)
                .distributionAlgorithm("partitionDistribution")
                .dataNodesAutoAdjustScaleDown(2)
                .dataNodesAutoAdjustScaleUp(3)
                .filter("filter")
                .storageProfiles("default")
                .consistencyMode("HIGH_AVAILABILITY")
                .build();
        CreateFromDefinitionImpl query1 = new CreateFromDefinitionImpl(null).from(zoneDefinition);
        String sqlZoneFromDefinition = query1.toString();

        TableDefinition tableDefinition = TableDefinition.builder("pojo_value_test")
                .ifNotExists()
                .key(PojoKey.class)
                .value(PojoValue.class)
                .colocateBy("id", "id_str")
                .zone(zoneDefinition.zoneName())
                .index("ix_pojo", IndexType.DEFAULT, column("f_name"), column("l_name").desc())
                .build();
        CreateFromDefinitionImpl query2 = new CreateFromDefinitionImpl(null).from(tableDefinition);
        String sqlTableFromDefinition = query2.toString();

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
                is("CREATE ZONE IF NOT EXISTS ZONE_TEST WITH STORAGE_PROFILES='default', PARTITIONS=1, REPLICAS=5, QUORUM_SIZE=2,"
                        + " DISTRIBUTION_ALGORITHM='partitionDistribution',"
                        + " DATA_NODES_AUTO_ADJUST_SCALE_UP=3, DATA_NODES_AUTO_ADJUST_SCALE_DOWN=2,"
                        + " DATA_NODES_FILTER='filter', CONSISTENCY_MODE='HIGH_AVAILABILITY';"
                        + System.lineSeparator()
                        + "CREATE TABLE IF NOT EXISTS PUBLIC.POJO_VALUE_TEST (ID INT, F_NAME VARCHAR, L_NAME VARCHAR, STR VARCHAR,"
                        + " PRIMARY KEY (ID)) COLOCATE BY (ID, ID_STR) ZONE ZONE_TEST;"
                        + System.lineSeparator()
                        + "CREATE INDEX IF NOT EXISTS IX_POJO ON PUBLIC.POJO_VALUE_TEST (F_NAME, L_NAME DESC);")
        );
    }

    @Test
    void createFromKeyValueClasses() {
        // key class fields (annotated only) is a composite primary keys
        CreateFromAnnotationsImpl query = createTable().processKeyValueClasses(PojoKey.class, PojoValue.class);
        assertThat(
                query.toString(),
                is("CREATE ZONE IF NOT EXISTS ZONE_TEST WITH STORAGE_PROFILES='default', PARTITIONS=1, REPLICAS=5, QUORUM_SIZE=2,"
                        + " DISTRIBUTION_ALGORITHM='partitionDistribution',"
                        + " DATA_NODES_AUTO_ADJUST_SCALE_UP=3, DATA_NODES_AUTO_ADJUST_SCALE_DOWN=2,"
                        + " DATA_NODES_FILTER='filter', CONSISTENCY_MODE='HIGH_AVAILABILITY';"
                        + System.lineSeparator()
                        + "CREATE TABLE IF NOT EXISTS PUBLIC.POJO_VALUE_TEST (ID INT, ID_STR VARCHAR(20), F_NAME VARCHAR, L_NAME VARCHAR,"
                        + " STR VARCHAR, PRIMARY KEY (ID, ID_STR)) COLOCATE BY (ID, ID_STR) ZONE ZONE_TEST;"
                        + System.lineSeparator()
                        + "CREATE INDEX IF NOT EXISTS IX_POJO ON PUBLIC.POJO_VALUE_TEST (F_NAME, L_NAME DESC);")
        );
    }

    @Test
    void createFromRecordClass() {
        CreateFromAnnotationsImpl query = createTable().processRecordClass(Pojo.class);
        assertThat(
                query.toString(),
                is("CREATE ZONE IF NOT EXISTS ZONE_TEST WITH STORAGE_PROFILES='default', PARTITIONS=1, REPLICAS=5, QUORUM_SIZE=2,"
                        + " DISTRIBUTION_ALGORITHM='partitionDistribution',"
                        + " DATA_NODES_AUTO_ADJUST_SCALE_UP=3, DATA_NODES_AUTO_ADJUST_SCALE_DOWN=2,"
                        + " DATA_NODES_FILTER='filter', CONSISTENCY_MODE='STRONG_CONSISTENCY';"
                        + System.lineSeparator()
                        + "CREATE TABLE IF NOT EXISTS PUBLIC.POJO_TEST"
                        + " (ID INT, ID_STR VARCHAR(20), F_NAME varchar(20) not null default 'a',"
                        + " L_NAME VARCHAR, STR VARCHAR, PRIMARY KEY (ID, ID_STR))"
                        + " COLOCATE BY (ID, ID_STR) ZONE ZONE_TEST;"
                        + System.lineSeparator()
                        + "CREATE INDEX IF NOT EXISTS IX_POJO ON PUBLIC.POJO_TEST (F_NAME, L_NAME DESC);")
        );
    }

    @Test
    void createFromRecordQuoted() {
        CreateFromAnnotationsImpl query = createTable().processRecordClass(PojoQuoted.class);
        assertThat(
                query.toString(),
                is("CREATE ZONE IF NOT EXISTS \"zone test\" WITH STORAGE_PROFILES='default', PARTITIONS=1, REPLICAS=3,"
                        + " DISTRIBUTION_ALGORITHM='partitionDistribution',"
                        + " DATA_NODES_AUTO_ADJUST_SCALE_UP=3, DATA_NODES_AUTO_ADJUST_SCALE_DOWN=2,"
                        + " DATA_NODES_FILTER='filter', CONSISTENCY_MODE='STRONG_CONSISTENCY';"
                        + System.lineSeparator()
                        + "CREATE TABLE IF NOT EXISTS \"sche ma\".\"pojo test\""
                        + " (ID INT, \"id str\" VARCHAR(20), \"f name\" varchar(20) not null default 'a',"
                        + " \"l name\" VARCHAR, STR VARCHAR, PRIMARY KEY (ID, \"id str\"))"
                        + " COLOCATE BY (ID, \"id str\") ZONE \"zone test\";"
                        + System.lineSeparator()
                        + "CREATE INDEX IF NOT EXISTS \"ix pojo\" ON \"sche ma\".\"pojo test\" (\"f name\", \"l name\" DESC);")
        );
    }

    @Test
    void createFromKeyValueClassesInvalid() {
        assertThrows(IllegalArgumentException.class, () -> createTable().processKeyValueClasses(Integer.class, PojoValueInvalid.class));
    }

    @Test
    void nameGeneration() {
        CreateFromAnnotationsImpl query = createTable().processRecordClass(NameGeneration.class);
        assertThat(
                query.toString(),
                is("CREATE TABLE IF NOT EXISTS PUBLIC.NAMEGENERATION (COL1 INT, COL2 VARCHAR);"
                        + System.lineSeparator()
                        + "CREATE INDEX IF NOT EXISTS IX_COL1_COL2 ON PUBLIC.NAMEGENERATION (COL1, COL2);")
        );
    }

    @Test
    void primaryKey() {
        CreateFromAnnotationsImpl query = createTable().processRecordClass(PkSort.class);
        assertThat(
                query.toString(),
                is("CREATE TABLE IF NOT EXISTS PUBLIC.PKSORT (ID INT, PRIMARY KEY USING SORTED (ID DESC));")
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

    @Test
    void allColumnTypes() {
        CreateFromAnnotationsImpl query = createTable().processRecordClass(AllColumnsPojo.class);
        assertThat(
                query.toString(),
                is("CREATE TABLE IF NOT EXISTS PUBLIC.ALLCOLUMNSPOJO ("
                        + "STR VARCHAR, BYTECOL TINYINT, SHORTCOL SMALLINT, INTCOL INT, LONGCOL BIGINT, FLOATCOL REAL, "
                        + "DOUBLECOL DOUBLE, DECIMALCOL DECIMAL, BOOLCOL BOOLEAN, BYTESCOL VARBINARY, UUIDCOL UUID, "
                        + "DATECOL DATE, TIMECOL TIME, DATETIMECOL TIMESTAMP, INSTANTCOL TIMESTAMP WITH LOCAL TIME ZONE, "
                        + "PRIMARY KEY (STR));")
        );
    }

    @Test
    void inheritance() {
        // Record class
        CreateFromAnnotationsImpl fromAnnotations = createTable().processRecordClass(PojoValueExtended.class);
        String sqlFromAnnotations = fromAnnotations.toString();

        assertThat(
                sqlFromAnnotations,
                is("CREATE TABLE IF NOT EXISTS PUBLIC.POJO_VALUE_EXTENDED_TEST ("
                        + "F_NAME_EXTENDED VARCHAR, F_NAME VARCHAR, L_NAME VARCHAR, STR VARCHAR);"
                        + System.lineSeparator()
                        + "CREATE INDEX IF NOT EXISTS IX_POJO ON PUBLIC.POJO_VALUE_EXTENDED_TEST (F_NAME, L_NAME DESC, F_NAME_EXTENDED);")
        );

        TableDefinition definition = TableDefinition.builder("pojo_value_extended_test")
                .ifNotExists()
                .record(PojoValueExtended.class)
                .index("ix_pojo", IndexType.DEFAULT, column("f_name"), column("l_name").desc(), column("f_name_extended"))
                .build();
        CreateFromDefinitionImpl fromDefinition = new CreateFromDefinitionImpl(null).from(definition);
        String sqlFromDefinition = fromDefinition.toString();

        assertThat(
                sqlFromAnnotations,
                is(sqlFromDefinition)
        );

        // Key Value class
        fromAnnotations = createTable().processKeyValueClasses(PojoKeyExtended.class, PojoValueExtended.class);
        sqlFromAnnotations = fromAnnotations.toString();

        assertThat(
                sqlFromAnnotations,
                is("CREATE TABLE IF NOT EXISTS PUBLIC.POJO_VALUE_EXTENDED_TEST ("
                        + "ID_STR_EXTENDED VARCHAR(20), ID INT, ID_STR VARCHAR(20), F_NAME_EXTENDED VARCHAR, F_NAME VARCHAR, "
                        + "L_NAME VARCHAR, STR VARCHAR, PRIMARY KEY (ID_STR_EXTENDED, ID, ID_STR));"
                        + System.lineSeparator()
                        + "CREATE INDEX IF NOT EXISTS IX_POJO ON PUBLIC.POJO_VALUE_EXTENDED_TEST (F_NAME, L_NAME DESC, F_NAME_EXTENDED);")
        );

        definition = TableDefinition.builder("pojo_value_extended_test")
                .ifNotExists()
                .key(PojoKeyExtended.class)
                .value(PojoValueExtended.class)
                .index("ix_pojo", IndexType.DEFAULT, column("f_name"), column("l_name").desc(), column("f_name_extended"))
                .build();
        fromDefinition = new CreateFromDefinitionImpl(null).from(definition);
        sqlFromDefinition = fromDefinition.toString();

        assertThat(
                sqlFromAnnotations,
                is(sqlFromDefinition)
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
    @Table(
            value = "pojo_value_test",
            zone = @Zone(
                    value = "zone_test",
                    partitions = 1,
                    replicas = 5,
                    quorumSize = 2,
                    distributionAlgorithm = "partitionDistribution",
                    dataNodesAutoAdjustScaleDown = 2,
                    dataNodesAutoAdjustScaleUp = 3,
                    filter = "filter",
                    storageProfiles = "default",
                    consistencyMode = "HIGH_AVAILABILITY"
            ),
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
            zone = @Zone(
                    value = "zone_test",
                    partitions = 1,
                    replicas = 5,
                    quorumSize = 2,
                    distributionAlgorithm = "partitionDistribution",
                    dataNodesAutoAdjustScaleDown = 2,
                    dataNodesAutoAdjustScaleUp = 3,
                    filter = "filter",
                    storageProfiles = "default",
                    consistencyMode = "STRONG_CONSISTENCY"
            ),
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

    @SuppressWarnings("unused")
    @Table(
            value = "pojo_invalid_test",
            zone = @Zone(
                    value = "zone_test",
                    partitions = 1,
                    replicas = 3,
                    distributionAlgorithm = "partitionDistribution",
                    dataNodesAutoAdjustScaleDown = 2,
                    dataNodesAutoAdjustScaleUp = 3,
                    filter = "filter",
                    storageProfiles = "default",
                    consistencyMode = "MY_CONSISTENCY"
            ),
            colocateBy = {@ColumnRef("id"), @ColumnRef("id_str")},
            indexes = @Index(value = "ix_pojo", columns = {
                    @ColumnRef("f_name"),
                    @ColumnRef(value = "l_name", sort = SortOrder.DESC),
            })
    )
    private static class PojoValueInvalid {
        @Column("f_name")
        String firstName;

        @Column("l_name")
        String lastName;

        String str;
    }

    @Table(indexes = @Index(columns = {@ColumnRef("col1"), @ColumnRef("col2")}))
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

    @Table
    private static class AllColumnsPojo {
        @Id
        String str;
        Byte byteCol;
        Short shortCol;
        Integer intCol;
        Long longCol;
        Float floatCol;
        Double doubleCol;
        BigDecimal decimalCol;
        Boolean boolCol;
        byte[] bytesCol;
        UUID uuidCol;
        LocalDate dateCol;
        LocalTime timeCol;
        LocalDateTime datetimeCol;
        Instant instantCol;
    }

    private static class NoAnnotations {
    }

    @SuppressWarnings("unused")
    @Table(
            value = "\"pojo test\"",
            schemaName = "\"sche ma\"",
            zone = @Zone(
                    value = "zone test",
                    partitions = 1,
                    replicas = 3,
                    distributionAlgorithm = "partitionDistribution",
                    dataNodesAutoAdjustScaleDown = 2,
                    dataNodesAutoAdjustScaleUp = 3,
                    filter = "filter",
                    storageProfiles = "default",
                    consistencyMode = "STRONG_CONSISTENCY"
            ),
            colocateBy = {@ColumnRef("id"), @ColumnRef("id str")},
            indexes = @Index(value = "ix pojo", columns = {
                    @ColumnRef("f name"),
                    @ColumnRef(value = "l name", sort = SortOrder.DESC)
            })
    )
    static class PojoQuoted {
        @Id
        Integer id;

        @Id
        @Column(value = "id str", length = 20)
        String idStr;

        @Column(value = "f name", columnDefinition = "varchar(20) not null default 'a'")
        String firstName;

        @Column("l name")
        String lastName;

        String str;
    }

    static class PojoKeyExtended extends PojoKey {
        @Id
        @Column(value = "id_str_extended", length = 20)
        String idStrExtended;
    }

    @Table(
            value = "pojo_value_extended_test",
            indexes = @Index(value = "ix_pojo", columns = {
                    @ColumnRef("f_name"),
                    @ColumnRef(value = "l_name", sort = SortOrder.DESC),
                    @ColumnRef("f_name_extended"),
            })
    )
    static class PojoValueExtended extends PojoValue {
        @Column("f_name_extended")
        String firstNameExtended;
    }

    private static CreateFromAnnotationsImpl createTable() {
        return new CreateFromAnnotationsImpl(null);
    }
}
