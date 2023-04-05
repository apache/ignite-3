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

package org.apache.ignite.internal.sql.engine.benchmarks;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.sql.engine.framework.DataProvider;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.DataProviderFactory;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;

/**
 * {@link DataProviderFactory} that creates {@link DataProvider}s that generates a row of pseudo random data based on table column types
 * and then returns the same row multiple times.
 */
final class RepeatedRandomRowDataProviderFactory implements DataProviderFactory {

    private final int dataSize;

    RepeatedRandomRowDataProviderFactory(int dataSize) {
        this.dataSize = dataSize;
    }

    /** {@inheritDoc} **/
    @Override
    public DataProvider<Object[]> createDataProvider(String tableName, List<ColumnDescriptor> columns) {
        Object[] row = columns.stream().map(c ->  generateValueByType(1, c.physicalType().spec())).toArray();

        return DataProvider.fromRow(row, dataSize);
    }

    private static Object generateValueByType(int i, NativeTypeSpec type) {
        switch (type) {
            case INT8:
                return (byte) i;
            case INT16:
                return (short) i;
            case INT32:
                return i;
            case INT64:
                return (long) i;
            case FLOAT:
                return (float) i + ((float) i / 1000);
            case DOUBLE:
                return (double) i + ((double) i / 1000);
            case STRING:
                return "str_" + i;
            case BYTES:
                return new byte[]{(byte) i, (byte) (i + 1), (byte) (i + 2)};
            case DECIMAL:
                return BigDecimal.valueOf((double) i + ((double) i / 1000));
            case NUMBER:
                return BigInteger.valueOf(i);
            case UUID:
                return new UUID(i, i);
            case BITMASK:
                return new byte[]{(byte) i};
            case DATETIME:
                return LocalDateTime.of(
                        (LocalDate) Objects.requireNonNull(generateValueByType(i, NativeTypeSpec.DATE)),
                        (LocalTime) Objects.requireNonNull(generateValueByType(i, NativeTypeSpec.TIME))
                );
            case TIMESTAMP:
                return Instant.from((LocalDateTime) Objects.requireNonNull(generateValueByType(i, NativeTypeSpec.DATETIME)));
            case DATE:
                return LocalDate.of(2022, 01, 01).plusDays(i);
            case TIME:
                return LocalTime.of(0, 00, 00).plusSeconds(i);
            default:
                throw new IllegalArgumentException("unsupported type " + type);
        }
    }
}
