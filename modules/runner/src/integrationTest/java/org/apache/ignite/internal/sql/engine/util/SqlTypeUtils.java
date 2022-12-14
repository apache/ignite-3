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

package org.apache.ignite.internal.sql.engine.util;

import org.apache.ignite.sql.SqlColumnType;

/** Utility class for work with SQL types. */
public class SqlTypeUtils {
    /** Converting  from {@code SqlColumnType} type to SQL type name. */
    public static String toSqlType(SqlColumnType sqlColumnType) {
        switch (sqlColumnType) {
            case BOOLEAN:
                return "BOOLEAN";
            case INT8:
                return "TINYINT";
            case INT16:
                return "SMALLINT";
            case INT32:
                return "INTEGER";
            case INT64:
                return "BIGINT";
            case FLOAT:
                return "REAL";
            case DOUBLE:
                return "DOUBLE";
            case DECIMAL:
                return "DECIMAL";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case DATETIME:
                return "DATETIME";
            case TIMESTAMP:
                return "TIMESTAMP";
            case UUID:
                return "UUID";
            case BITMASK:
                return "BITMASK";
            case STRING:
                return "VARCHAR";
            case BYTE_ARRAY:
                return "VARBINARY";
            case PERIOD:
                return "INTERVAL";
            case DURATION:
                return "DURATION";
            case NUMBER:
                return "NUMBER";
            case NULL:
                return "NULL";
            default:
                throw new IllegalArgumentException("Unsupported type " + sqlColumnType);
        }
    }
}
