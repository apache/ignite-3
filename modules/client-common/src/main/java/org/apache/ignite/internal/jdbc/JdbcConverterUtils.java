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

package org.apache.ignite.internal.jdbc;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.sql.ColumnType;

/**
 * Utils for type conversion related to JDBC.
 */
public class JdbcConverterUtils {
    /**
     * Column type to Java class for JDBC.
     */
    public static Class<?> columnTypeToJdbcClass(ColumnType type) {
        assert type != null;

        switch (type) {
            case DATE:
                return Date.class;
            case TIME:
                return Time.class;
            case DATETIME:
            case TIMESTAMP:
                return Timestamp.class;
            default:
                return type.javaClass();
        }
    }

    /**
     * Derive value from BinaryTuple by given column number, column type and scale for decimal type.
     *
     * @param columnType Type of column in binaryTuple by index idx.
     * @param binaryTuple BinaryTuple to extract value.
     * @param idx Index of column in binaryTuple.
     * @param decimalScale Scale for decimal column. If the column is of a different type, then the specific value does not matter.
     * @return Derived value. The value can be {@code null}.
     */
    public static Object deriveValueFromBinaryTuple(ColumnType columnType, BinaryTuple binaryTuple, int idx, int decimalScale) {
        switch (columnType) {
            case INT8:
                return binaryTuple.byteValue(idx);

            case INT16:
                return binaryTuple.shortValue(idx);

            case INT32:
                return binaryTuple.intValue(idx);

            case INT64:
                return binaryTuple.longValue(idx);

            case FLOAT:
                return binaryTuple.floatValue(idx);

            case DOUBLE:
                return binaryTuple.doubleValue(idx);

            case DECIMAL:
                return binaryTuple.decimalValue(idx, decimalScale);

            case UUID:
                return binaryTuple.uuidValue(idx);

            case STRING:
                return binaryTuple.stringValue(idx);

            case BYTE_ARRAY:
                return binaryTuple.bytesValue(idx);

            case BITMASK:
                return binaryTuple.bitmaskValue(idx);

            case DATE:
                return binaryTuple.dateValue(idx);

            case TIME:
                return binaryTuple.timeValue(idx);

            case DATETIME:
                return binaryTuple.dateTimeValue(idx);

            case TIMESTAMP:
                return binaryTuple.timestampValue(idx);

            case NUMBER:
                return binaryTuple.numberValue(idx);

            case BOOLEAN:
                return binaryTuple.booleanValue(idx);

            case DURATION:
                return binaryTuple.durationValue(idx);

            case PERIOD:
                return binaryTuple.periodValue(idx);

            default:
                throw new IllegalArgumentException("Unsupported Column type " + columnType);
        }
    }

}
