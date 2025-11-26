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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.sql.ColumnType;

/**
 * Random {@link CatalogTableColumnDescriptor}S for testing.
 */
final class TestTableColumnDescriptors {

    private static final ZoneId ZONE = ZoneId.of("Europe/Paris");

    /**
     * Generates a list of {@link CatalogTableColumnDescriptor}s of the given version.
     *
     * @param state Random state,
     * @return A list of descriptors.
     */
    static List<CatalogTableColumnDescriptor> columns(TestDescriptorState state) {
        List<CatalogTableColumnDescriptor> list = new ArrayList<>();
        Random random = state.random();

        for (ColumnType columnType : NativeType.nativeTypes()) {
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
}
