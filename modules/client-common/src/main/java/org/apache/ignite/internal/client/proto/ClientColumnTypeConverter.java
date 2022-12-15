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

package org.apache.ignite.internal.client.proto;

import org.apache.ignite.sql.ColumnType;

/**
 * SQL column type utils.
 */
public class ClientColumnTypeConverter {
    /**
     * Converts column type to wire code.
     *
     * @param columnType Column type.
     * @return Wire code.
     */
    public static int columnTypeToOrdinal(ColumnType columnType) {
        switch (columnType) {
            case BOOLEAN:
                return 0;

            case INT8:
                return 1;

            case INT16:
                return 2;

            case INT32:
                return 3;

            case INT64:
                return 4;

            case FLOAT:
                return 5;

            case DOUBLE:
                return 6;

            case DECIMAL:
                return 7;

            case DATE:
                return 8;

            case TIME:
                return 9;

            case DATETIME:
                return 10;

            case TIMESTAMP:
                return 11;

            case UUID:
                return 12;

            case BITMASK:
                return 13;

            case STRING:
                return 14;

            case BYTE_ARRAY:
                return 15;

            case PERIOD:
                return 16;

            case DURATION:
                return 17;

            case NUMBER:
                return 18;

            case NULL:
                return 19;

            default:
                throw new IllegalArgumentException("Invalid column type: " + columnType);
        }
    }

    /**
     * Converts wire type code to column type.
     *
     * @param ordinal Type code.
     * @return Column type.
     */
    public static ColumnType ordinalToColumnType(int ordinal) {
        switch (ordinal) {
            case 0:
                return ColumnType.BOOLEAN;

            case 1:
                return ColumnType.INT8;

            case 2:
                return ColumnType.INT16;

            case 3:
                return ColumnType.INT32;

            case 4:
                return ColumnType.INT64;

            case 5:
                return ColumnType.FLOAT;

            case 6:
                return ColumnType.DOUBLE;

            case 7:
                return ColumnType.DECIMAL;

            case 8:
                return ColumnType.DATE;

            case 9:
                return ColumnType.TIME;

            case 10:
                return ColumnType.DATETIME;

            case 11:
                return ColumnType.TIMESTAMP;

            case 12:
                return ColumnType.UUID;

            case 13:
                return ColumnType.BITMASK;

            case 14:
                return ColumnType.STRING;

            case 15:
                return ColumnType.BYTE_ARRAY;

            case 16:
                return ColumnType.PERIOD;

            case 17:
                return ColumnType.DURATION;

            case 18:
                return ColumnType.NUMBER;

            case 19:
                return ColumnType.NULL;

            default:
                throw new IllegalArgumentException("Invalid column type code: " + ordinal);
        }
    }
}
