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
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

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
     * Derives value from {@link BinaryTupleReader} by given column index, column type, and scale for decimal type.
     *
     * @param columnType An expected type of the field.
     * @param binaryTuple A tuple reader to derive value from.
     * @param idx An index of a field of interest.
     * @param decimalScale A scale for decimal field. If the field is of a different type, then the specific value does not matter.
     * @return Derived value. The value can be {@code null}.
     */
    public static @Nullable Object deriveValueFromBinaryTuple(ColumnType columnType, BinaryTupleReader binaryTuple, int idx,
            int decimalScale) {
        switch (columnType) {
            case INT8:
                return binaryTuple.byteValueBoxed(idx);

            case INT16:
                return binaryTuple.shortValueBoxed(idx);

            case INT32:
                return binaryTuple.intValueBoxed(idx);

            case INT64:
                return binaryTuple.longValueBoxed(idx);

            case FLOAT:
                return binaryTuple.floatValueBoxed(idx);

            case DOUBLE:
                return binaryTuple.doubleValueBoxed(idx);

            case DECIMAL:
                return binaryTuple.decimalValue(idx, decimalScale);

            case UUID:
                return binaryTuple.uuidValue(idx);

            case STRING:
                return binaryTuple.stringValue(idx);

            case BYTE_ARRAY:
                return binaryTuple.bytesValue(idx);

            case DATE:
                return binaryTuple.dateValue(idx);

            case TIME:
                return binaryTuple.timeValue(idx);

            case DATETIME:
                return binaryTuple.dateTimeValue(idx);

            case TIMESTAMP:
                return binaryTuple.timestampValue(idx);

            case BOOLEAN:
                return binaryTuple.booleanValueBoxed(idx);

            case DURATION:
                return binaryTuple.durationValue(idx);

            case PERIOD:
                return binaryTuple.periodValue(idx);

            case NULL:
                return null;

            default:
                throw new IllegalArgumentException("Unsupported Column type " + columnType);
        }
    }

}
