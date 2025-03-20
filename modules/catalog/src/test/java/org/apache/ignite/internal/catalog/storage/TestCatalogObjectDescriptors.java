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

package org.apache.ignite.internal.catalog.storage;

import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfileDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfilesDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor.SystemViewType;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.sql.ColumnType;

/**
 * Random catalog object descriptors for testing.
 */
final class TestCatalogObjectDescriptors {

    private static final List<CatalogStorageProfilesDescriptor> STORAGE_PROFILES = List.of(
            new CatalogStorageProfilesDescriptor(
                    List.of(new CatalogStorageProfileDescriptor("S1"))
            ),
            new CatalogStorageProfilesDescriptor(
                    List.of(new CatalogStorageProfileDescriptor("S1"), new CatalogStorageProfileDescriptor("S2"))
            )
    );

    private static final ZoneId ZONE = ZoneId.of("Europe/Paris");

    static List<CatalogZoneDescriptor> zones(TestDescriptorState state) {
        List<CatalogZoneDescriptor> list = new ArrayList<>();

        list.add(new CatalogZoneDescriptor(
                state.id(),
                state.name("ZONE"),
                1,
                2,
                3,
                4,
                5,
                CatalogUtils.DEFAULT_FILTER,
                STORAGE_PROFILES.get(0),
                ConsistencyMode.STRONG_CONSISTENCY
        ));

        list.add(new CatalogZoneDescriptor(
                state.id(),
                state.name("ZONE"),
                5,
                4,
                3,
                2,
                1,
                "$[?(@.region == \"europe\")]",
                STORAGE_PROFILES.get(0),
                ConsistencyMode.HIGH_AVAILABILITY
        ));

        list.add(new CatalogZoneDescriptor(state.id(), state.name("ZONE"),
                5,
                4,
                3,
                2,
                1,
                CatalogUtils.DEFAULT_FILTER,
                STORAGE_PROFILES.get(1),
                ConsistencyMode.HIGH_AVAILABILITY
        ));

        return list;
    }

    static List<CatalogTableDescriptor> tables(TestDescriptorState state) {
        List<CatalogTableDescriptor> list = new ArrayList<>();

        CatalogTableDescriptor table1 = new CatalogTableDescriptor(
                state.id(),
                state.id(),
                state.id(),
                state.name("TABLE"),
                100,
                columns(state),
                List.of("C0"),
                null,
                "S1"
        );

        list.add(table1);
        list.add(list.get(0).newDescriptor(
                table1.name() + "_1", 2,
                columns(state).subList(0, 10),
                hybridTimestamp(1232L),
                "S1")
        );
        list.add(list.get(list.size() - 1).newDescriptor(table1.name() + "_2", 3, columns(state).subList(0, 20), hybridTimestamp(21232L),
                "S1"));

        CatalogTableDescriptor table2 = new CatalogTableDescriptor(
                state.id(),
                state.id(),
                state.id(),
                state.name("TABLE"),
                101,
                columns(state), List.of("C4"), null,
                "S2"
        );

        list.add(table2);
        list.add(list.get(list.size() - 1).newDescriptor(
                table2.name() + "_1", 2,
                columns(state).subList(0, 10),
                hybridTimestamp(4567L), "S2")
        );
        list.add(list.get(list.size() - 1).newDescriptor(
                table2.name() + "_2", 3,
                columns(state).subList(0, 20),
                hybridTimestamp(8833L),
                "S2")
        );

        CatalogTableDescriptor table3 = new CatalogTableDescriptor(
                state.id(),
                state.id(),
                state.id(),
                state.name("TABLE"),
                102,
                columns(state),
                List.of("C1", "C2", "C3"),
                List.of("C2", "C3"),
                "S3"
        );
        list.add(table3);
        list.add(list.get(list.size() - 1).newDescriptor(
                table3.name() + "_1",
                2,
                columns(state),
                hybridTimestamp(123234L),
                "S4")
        );

        return list;
    }

    private static CatalogIndexColumnDescriptor column(String name, CatalogColumnCollation collation) {
        return new CatalogIndexColumnDescriptor(name, collation);
    }

    static List<CatalogSortedIndexDescriptor> sortedIndices(TestDescriptorState state) {
        List<CatalogSortedIndexDescriptor> list = new ArrayList<>();

        for (var unique : new boolean[]{true, false}) {
            for (var indexStatus : CatalogIndexStatus.values()) {
                for (var isCreatedWithTable : new boolean[]{false, true}) {
                    List<CatalogIndexColumnDescriptor> columns = Arrays.asList(
                            column("C1", CatalogColumnCollation.ASC_NULLS_FIRST),
                            column("C2", CatalogColumnCollation.ASC_NULLS_LAST),
                            column("C3", CatalogColumnCollation.DESC_NULLS_FIRST),
                            column("C4", CatalogColumnCollation.DESC_NULLS_LAST)
                    );
                    Collections.shuffle(columns, state.random());

                    list.add(new CatalogSortedIndexDescriptor(
                            state.id(),
                            state.name("SORTED_IDX"),
                            1000 + list.size(),
                            unique,
                            indexStatus,
                            columns,
                            isCreatedWithTable));
                }
            }
        }

        return list;
    }

    static List<CatalogHashIndexDescriptor> hashIndices(TestDescriptorState state) {
        List<CatalogHashIndexDescriptor> list = new ArrayList<>();

        for (var unique : new boolean[]{true, false}) {
            for (var indexStatus : CatalogIndexStatus.values()) {
                for (var isCreatedWithTable : new boolean[]{false, true}) {
                    List<String> columns = Arrays.asList("C1", "C2");
                    Collections.shuffle(columns, state.random());

                    list.add(new CatalogHashIndexDescriptor(
                            state.id(),
                            state.name("HASH_IDX"),
                            1000 + list.size(),
                            unique,
                            indexStatus,
                            columns,
                            isCreatedWithTable));
                }
            }
        }

        return list;
    }

    static List<CatalogSystemViewDescriptor> systemViews(TestDescriptorState state) {
        List<CatalogSystemViewDescriptor> list = new ArrayList<>();

        list.add(new CatalogSystemViewDescriptor(
                state.id(),
                state.id(),
                state.name("SYS_VIEW"),
                columns(state),
                SystemViewType.NODE, hybridTimestamp(90323L))
        );
        list.add(new CatalogSystemViewDescriptor(
                state.id(),
                state.id(),
                state.name("SYS_VIEW"),
                columns(state),
                SystemViewType.CLUSTER,
                hybridTimestamp(213232L))
        );

        return list;
    }

    static List<CatalogTableColumnDescriptor> columns(TestDescriptorState state) {
        List<CatalogTableColumnDescriptor> list = new ArrayList<>();
        Random random = state.random();

        for (ColumnType columnType : ColumnType.values()) {
            if (columnType == ColumnType.NULL || columnType == ColumnType.PERIOD || columnType == ColumnType.DURATION) {
                continue;
            }
            for (var nullable : new boolean[]{true, false}) {
                if (columnType.lengthAllowed()) {
                    addColumnsWithLength(columnType, nullable, random, list);
                } else if (columnType.precisionAllowed() && columnType.scaleAllowed()) {
                    addColumnsWithPrecisionScale(columnType, nullable, random, list);
                } else if (columnType.precisionAllowed()) {
                    addColumnsWithPrecision(columnType, nullable, random, list);
                } else {
                    addColumns(columnType, nullable, random, list);
                }
            }
        }

        return list;
    }

    private static void addColumnsWithLength(
            ColumnType columnType,
            boolean nullable,
            Random random,
            List<CatalogTableColumnDescriptor> list
    ) {
        int minLength = CatalogUtils.getMinLength(columnType);
        int maxLength = CatalogUtils.getMaxLength(columnType);

        for (int length : new int[]{minLength, minLength + (maxLength - minLength) / 2, maxLength}) {
            int l = Math.min(length, 100);
            Object value = generateValue(random, columnType, 0, 0, l);

            for (var defaultValue : List.of(DefaultValue.constant(null), DefaultValue.constant(value))) {
                var columnName = "C" + list.size();
                var col = new CatalogTableColumnDescriptor(
                        columnName,
                        columnType,
                        nullable,
                        0,
                        0,
                        length,
                        defaultValue
                );
                list.add(col);
            }
        }
    }

    private static void addColumnsWithPrecisionScale(
            ColumnType columnType,
            boolean nullable,
            Random random,
            List<CatalogTableColumnDescriptor> list
    ) {
        int minPrecision = CatalogUtils.getMinPrecision(columnType);
        int maxPrecision = CatalogUtils.getMaxPrecision(columnType);
        int minScale = CatalogUtils.getMinScale(columnType);
        int maxScale = CatalogUtils.getMaxScale(columnType);

        for (int precision : new int[]{minPrecision, minPrecision + (maxPrecision - minPrecision) / 2, maxPrecision}) {
            for (int scale : new int[]{minScale, minScale + (maxScale - minScale) / 2, maxScale}) {
                int p = Math.min(precision, 100);
                int s = Math.min(scale, 100);

                Object value = generateValue(random, columnType, p, s, 0);
                for (var defaultValue : List.of(DefaultValue.constant(null), DefaultValue.constant(value))) {
                    var columnName = "C" + list.size();
                    var col = new CatalogTableColumnDescriptor(
                            columnName,
                            columnType,
                            nullable,
                            0,
                            precision,
                            scale,
                            defaultValue
                    );
                    list.add(col);
                }
            }
        }
    }

    private static void addColumnsWithPrecision(ColumnType columnType,
            boolean nullable,
            Random random,
            List<CatalogTableColumnDescriptor> list
    ) {
        int minPrecision = CatalogUtils.getMinPrecision(columnType);
        int maxPrecision = CatalogUtils.getMaxPrecision(columnType);

        for (int precision : new int[]{minPrecision, minPrecision + (maxPrecision - minPrecision) / 2, maxPrecision}) {
            int p = Math.min(precision, 100);
            Object value = generateValue(random, columnType, p, 0, 0);

            for (var defaultValue : List.of(DefaultValue.constant(null), DefaultValue.constant(value))) {
                var columnName = "C" + list.size();
                var col = new CatalogTableColumnDescriptor(
                        columnName,
                        columnType,
                        nullable,
                        0,
                        precision,
                        0,
                        defaultValue
                );
                list.add(col);
            }
        }
    }

    private static void addColumns(ColumnType columnType, boolean nullable, Random random, List<CatalogTableColumnDescriptor> list) {
        Object value = generateValue(random, columnType, 0, 0, 0);
        for (var defaultValue : List.of(DefaultValue.constant(null), DefaultValue.constant(value))) {
            var columnName = "C" + list.size();
            var col = new CatalogTableColumnDescriptor(
                    columnName,
                    columnType,
                    nullable,
                    0,
                    0,
                    0,
                    defaultValue
            );
            list.add(col);
        }
    }

    private static Object generateValue(Random random, ColumnType columnType, int precision, int scale, int length) {
        switch (columnType) {
            case NULL:
                throw new IllegalArgumentException("Unsupported: " + columnType);
            case BOOLEAN:
                return random.nextBoolean();
            case INT8:
                return (byte) random.nextInt();
            case INT16:
                return (short) random.nextInt();
            case INT32:
                return random.nextInt();
            case INT64:
                return random.nextLong();
            case FLOAT:
                return (float) random.nextInt(1000);
            case DOUBLE:
                return (double) random.nextInt(1000);
            case DECIMAL:
                return IgniteTestUtils.randomBigDecimal(random, precision, scale);
            case DATE:
                return LocalDate.of(1900 + random.nextInt(1000), 1 + random.nextInt(12), 1 + random.nextInt(28));
            case TIME:
                return IgniteTestUtils.randomTime(random, precision);
            case DATETIME:
                return LocalDateTime.of(
                        (LocalDate) generateValue(random, ColumnType.DATE, precision, scale, length),
                        (LocalTime) generateValue(random, ColumnType.TIME, precision, scale, length)
                );
            case TIMESTAMP:
                LocalDateTime localDateTime = (LocalDateTime) generateValue(random, ColumnType.DATETIME, precision, scale, length);
                ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, ZONE);
                return Instant.from(zonedDateTime);
            case UUID:
                return new UUID(random.nextLong(), random.nextLong());
            case STRING:
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < length; i++) {
                    sb.append((char) ('a' + random.nextInt(26)));
                }
                return sb.toString();
            case BYTE_ARRAY:
                return IgniteTestUtils.randomBytes(random, length);
            case PERIOD:
            case DURATION:
            default:
                throw new IllegalArgumentException("Unsupported: " + columnType);
        }
    }

    static List<CatalogSchemaDescriptor> schemas(TestDescriptorState state) {
        List<CatalogSchemaDescriptor> list = new ArrayList<>();

        CatalogTableDescriptor[] tables = tables(state).toArray(new CatalogTableDescriptor[0]);
        CatalogIndexDescriptor[] indexes = Stream.concat(hashIndices(state).stream(), sortedIndices(state).stream())
                .toArray(CatalogIndexDescriptor[]::new);

        CatalogSystemViewDescriptor[] systemViews = systemViews(state).toArray(new CatalogSystemViewDescriptor[0]);

        list.add(new CatalogSchemaDescriptor(
                state.id(),
                state.name("schema"),
                new CatalogTableDescriptor[0],
                new CatalogIndexDescriptor[0],
                new CatalogSystemViewDescriptor[0],
                hybridTimestamp(123232L))
        );

        list.add(new CatalogSchemaDescriptor(
                state.id(),
                state.name("schema"),
                tables,
                new CatalogIndexDescriptor[0],
                new CatalogSystemViewDescriptor[0],
                hybridTimestamp(76765754L))
        );

        list.add(new CatalogSchemaDescriptor(
                state.id(),
                state.name("schema"),
                tables,
                indexes,
                new CatalogSystemViewDescriptor[0],
                hybridTimestamp(2212L))
        );

        list.add(new CatalogSchemaDescriptor(
                state.id(),
                state.name("schema"),
                tables,
                indexes,
                systemViews,
                hybridTimestamp(435546L))
        );

        return list;
    }

}
