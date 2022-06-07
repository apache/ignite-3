/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.sql;

import org.jetbrains.annotations.Nullable;

/**
 * Interface that provides methods for accessing column metadata.
 */
public interface ColumnMetadata {
    /**
     * Return column name in the result set.
     *
     * <p>Note: If row column does not represent any table column, then generated name will be
     * used.
     *
     * @return Column name.
     */
    String name();

    /**
     * Returns a class of column values.
     *
     * @return Value class.
     */
    Class<?> valueClass();

    /**
     * Returns SQL column type.
     *
     * @return Value type.
     */
    SqlColumnType type();

    /**
     * Returns SQL column precision or {@code -1} if precision is not applicable for the type.
     *
     * @return Number of decimal digits for exact numeric types; number of decimal digits in mantissa for approximate numeric types; number
     *     of decimal digits for fractional seconds of datetime types; length in characters for character types; length in bytes for binary
     *     types; length in bits for bit types; 1 for BOOLEAN; -1 if precision is not valid for the type.
     */
    int precision();

    /**
     * Returns SQL column scale or {@code -1} if scale is not applicable for this type.
     *
     * @return Number of digits of scale.
     */
    int scale();

    /**
     * Returns row column nullability flag.
     *
     * @return {@code true} if column is nullable, {@code false} otherwise.
     */
    boolean nullable();

    /**
     * Return column origin.
     *
     * @return Column origin or {@code null} if not applicable.
     */
    @Nullable ColumnOrigin origin();

    /**
     * Represent column origin.
     *
     * <p>Example:
     * <pre>
     *     SELECT SUM(price), category as cat, subcategory AS subcategory
     *       FROM Goods
     *      WHERE [condition]
     *      GROUP BY cat, subcategory
     * </pre>
     *
     * <p>Column origins:
     * <ul>
     * <li>SUM(price): null</li>
     * <li>cat: {"PUBLIC", "Goods", "category"}</li>
     * <li>subcategory: {"PUBLIC", "Goods", "subcategory"}</li>
     * </ul>
     */
    interface ColumnOrigin {
        /**
         * Return the column's table's schema.
         *
         * @return Schema name.
         */
        String schemaName();

        /**
         * Return the column's table name.
         *
         * @return Table name.
         */
        String tableName();

        /**
         * Return the column name.
         *
         * @return Table name.
         */
        String columnName();
    }
}
