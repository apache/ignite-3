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

package org.apache.ignite.table;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * Tuple represents an arbitrary set of columns whose values are accessible by column name.
 * Column name arguments passed to the tuple methods must use SQL-parser style notation; e.g.,
 * "myColumn" - column named "MYCOLUMN", "\"MyColumn\"" - "MyColumn", etc.
 *
 * <p>Provides a specialized method for some value-types to avoid boxing/unboxing.
 */
public interface Tuple extends Iterable<Object> {
    /**
     * Creates a tuple.
     *
     * @return A new tuple.
     */
    static Tuple create() {
        return new TupleImpl();
    }

    /**
     * Creates a tuple with a specified initial capacity.
     *
     * @param capacity Initial capacity.
     * @return A new tuple.
     */
    static Tuple create(int capacity) {
        return new TupleImpl(capacity);
    }

    /**
     * Creates a tuple from a given mapping.
     *
     * @param map Column values.
     * @return A new tuple.
     */
    static Tuple create(Map<String, Object> map) {
        TupleImpl tuple = new TupleImpl(map.size());

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            tuple.set(entry.getKey(), entry.getValue());
        }

        return tuple;
    }

    /**
     * Creates a tuple copy.
     *
     * @param tuple Tuple to copy.
     * @return A new tuple.
     */
    static Tuple copy(Tuple tuple) {
        return new TupleImpl(tuple);
    }

    /**
     * Returns a hash code value for the tuple.
     *
     * <p>The hash code of a tuple is the sum of the hash codes of each pair of column name and column value. Therefore, 
     * {@code m1.equals(m2)} implies that {@code m1.hashCode()==m2.hashCode()} for any tuples {@code m1} and {@code m2}, 
     * as required by the general contract of {@link Object#hashCode}.
     *
     * <p>The hash code of a pair of column name and column value {@code i} is:
     * <pre>(columnName(i).hashCode()) ^ (value(i)==null ? 0 : value(i).hashCode())</pre>
     *
     * @param tuple Tuple.
     * @return The hash code value for the tuple.
     */
    static int hashCode(Tuple tuple) {
        int hash = 0;

        for (int idx = 0; idx < tuple.columnCount(); idx++) {
            String columnName = tuple.columnName(idx);
            Object columnValue = tuple.value(idx);

            int columnValueHash = 0;

            if (columnValue != null) {
                if (columnValue instanceof byte[]) {
                    columnValueHash = Arrays.hashCode((byte[]) columnValue);
                } else {
                    columnValueHash = columnValue.hashCode();
                }
            }

            hash += columnName.hashCode() ^ columnValueHash;
        }

        return hash;
    }

    /**
     * Returns a hash code value for the tuple.
     *
     * @return The hash code value for the tuple.
     * @see #hashCode(Tuple)
     * @see Object#hashCode()
     */
    @Override
    int hashCode();

    /**
     * Compares tuples for equality.
     *
     * <p>Returns {@code true} if both tuples represent the same column name-to-value mapping.
     *
     * <p>This implementation first checks if both tuples is of same size. If not, it returns {@code false}. If yes, 
     * it iterates over columns of the first tuple and checks that the second tuple contains each mapping that the first 
     * one contains.  If the second tuple fails to contain such a mapping, {@code false} is returned.
     * If the iteration completes, {@code true} is returned.
     *
     * @param firstTuple  First tuple to compare.
     * @param secondTuple Second tuple to compare.
     * @return {@code true} if the first tuple is equal to the second tuple.
     */
    static boolean equals(Tuple firstTuple, Tuple secondTuple) {
        if (firstTuple == secondTuple) {
            return true;
        }

        int columns = firstTuple.columnCount();

        if (columns != secondTuple.columnCount()) {
            return false;
        }

        for (int idx = 0; idx < columns; idx++) {
            int idx2 = secondTuple.columnIndex(firstTuple.columnName(idx));

            if (idx2 < 0) {
                return false;
            }

            Object firstVal = firstTuple.value(idx);
            Object secondVal = secondTuple.value(idx2);

            if (!Objects.deepEquals(firstVal, secondVal)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Indicates whether another object is "equal to" the specified one.
     *
     * @return {@code true} if this object is the same as the specified one; {@code false} otherwise.
     * @see Tuple#equals(Tuple, Tuple)
     * @see Object#equals(Object)
     */
    @Override
    boolean equals(Object obj);

    /**
     * Gets a number of columns in the tuple.
     *
     * @return Number of columns.
     */
    int columnCount();

    /**
     * Gets a name of the column with the specified index.
     *
     * @param columnIndex Column index.
     * @return Column name.
     * @throws IndexOutOfBoundsException If a value for a column with the given index doesn't exists.
     */
    String columnName(int columnIndex);

    /**
     * Gets an index of the column with the specified name.
     *
     * @param columnName Column name in SQL-parser style notation; e.g., <br>
     *                   "myColumn" - "MYCOLUMN", returns the index of the column ignores case sensitivity, <br>
     *                   "\"MyColumn\"" - "MyColumn", returns the index of the column with respect to case sensitivity.
     * @return Column index, or {@code -1} if the column with the given name is not present.
     */
    int columnIndex(String columnName);

    /**
     * Gets a column value if the column with the specified name is present in the tuple; returns a default value otherwise.
     *
     * @param columnName Column name in SQL-parser style notation; e.g., <br>
     *                   "myColumn" - "MYCOLUMN", returns the index of the column ignores case sensitivity, <br>
     *                   "\"MyColumn\"" - "MyColumn", returns the index of the column with respect to case sensitivity.
     * @param defaultValue Default value.
     * @param <T>          Default value type.
     * @return Column value if the tuple contains a column with the specified name. Otherwise, {@code defaultValue}.
     */
    @Nullable <T> T valueOrDefault(String columnName, @Nullable T defaultValue);

    /**
     * Sets a column value.
     *
     * @param columnName Column name in SQL-parser style notation; e.g., <br>
     *                   "myColumn" - "MYCOLUMN", returns the index of the column ignores case sensitivity, <br>
     *                   "\"MyColumn\"" - "MyColumn", returns the index of the column with respect to case sensitivity.
     * @param value      Value to set.
     * @return {@code this} for chaining.
     */
    Tuple set(String columnName, @Nullable Object value);

    /**
     * Gets a column value for the given column name.
     *
     * @param columnName Column name in SQL-parser style notation; e.g., <br>
     *                   "myColumn" - "MYCOLUMN", returns the index of the column ignores case sensitivity, <br>
     *                   "\"MyColumn\"" - "MyColumn", returns the index of the column with respect to case sensitivity.
     * @param <T>        Value type.
     * @return Column value.
     * @throws IllegalArgumentException If no column with the given name exists.
     */
    @Nullable <T> T value(String columnName) throws IllegalArgumentException;

    /**
     * Gets a column value for the given column index.
     *
     * @param columnIndex Column index.
     * @param <T>         Value type.
     * @return Column value.
     * @throws IndexOutOfBoundsException If no column with the given index exists.
     */
    @Nullable <T> T value(int columnIndex);

    /**
     * Gets a {@code boolean} column value.
     *
     * @param columnName Column name in SQL-parser style notation; e.g., <br>
     *                   "myColumn" - "MYCOLUMN", returns the index of the column ignores case sensitivity, <br>
     *                   "\"MyColumn\"" - "MyColumn", returns the index of the column with respect to case sensitivity.
     * @return Column value.
     * @throws IllegalArgumentException If no column with given name exists.
     */
    boolean booleanValue(String columnName);

    /**
     * Gets {@code boolean} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If no column with the given index exists.
     */
    boolean booleanValue(int columnIndex);

    /**
     * Gets a {@code byte} column value.
     *
     * @param columnName Column name in SQL-parser style notation; e.g., <br>
     *                   "myColumn" - "MYCOLUMN", returns the index of the column ignores case sensitivity, <br>
     *                   "\"MyColumn\"" - "MyColumn", returns the index of the column with respect to case sensitivity.
     * @return Column value.
     * @throws IllegalArgumentException If no column with given name exists.
     */
    byte byteValue(String columnName);

    /**
     * Gets {@code byte} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If no column with the given index exists.
     */
    byte byteValue(int columnIndex);

    /**
     * Gets a {@code short} column value.
     *
     * @param columnName Column name in SQL-parser style notation; e.g., <br>
     *                   "myColumn" - "MYCOLUMN", returns the index of the column ignores case sensitivity, <br>
     *                   "\"MyColumn\"" - "MyColumn", returns the index of the column with respect to case sensitivity.
     * @return Column value.
     * @throws IllegalArgumentException If no column with the given name exists.
     */
    short shortValue(String columnName);

    /**
     * Gets a {@code short} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If no column with the given index exists.
     */
    short shortValue(int columnIndex);

    /**
     * Gets a {@code int} column value.
     *
     * @param columnName Column name in SQL-parser style notation; e.g., <br>
     *                   "myColumn" - "MYCOLUMN", returns the index of the column ignores case sensitivity, <br>
     *                   "\"MyColumn\"" - "MyColumn", returns the index of the column with respect to case sensitivity.
     * @return Column value.
     * @throws IllegalArgumentException If no column with the given name exists.
     */
    int intValue(String columnName);

    /**
     * Gets a {@code int} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If no column with the given index exists.
     */
    int intValue(int columnIndex);

    /**
     * Gets a {@code long} column value.
     *
     * @param columnName Column name in SQL-parser style notation; e.g., <br>
     *                   "myColumn" - "MYCOLUMN", returns the index of the column ignores case sensitivity, <br>
     *                   "\"MyColumn\"" - "MyColumn", returns the index of the column with respect to case sensitivity.
     * @return Column value.
     * @throws IllegalArgumentException If no column with the given name exists.
     */
    long longValue(String columnName);

    /**
     * Gets a {@code long} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If no column with the given index exists.
     */
    long longValue(int columnIndex);

    /**
     * Gets a {@code float} column value.
     *
     * @param columnName Column name in SQL-parser style notation; e.g., <br>
     *                   "myColumn" - "MYCOLUMN", returns the index of the column ignores case sensitivity, <br>
     *                   "\"MyColumn\"" - "MyColumn", returns the index of the column with respect to case sensitivity.
     * @return Column value.
     * @throws IllegalArgumentException If no column with the given name exists.
     */
    float floatValue(String columnName);

    /**
     * Gets a {@code float} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If no column with the given index exists.
     */
    float floatValue(int columnIndex);

    /**
     * Gets a {@code double} column value.
     *
     * @param columnName Column name in SQL-parser style notation; e.g., <br>
     *                   "myColumn" - "MYCOLUMN", returns the index of the column ignores case sensitivity, <br>
     *                   "\"MyColumn\"" - "MyColumn", returns the index of the column with respect to case sensitivity.
     * @return Column value.
     * @throws IllegalArgumentException If no column with the given name exists.
     */
    double doubleValue(String columnName);

    /**
     * Gets a {@code double} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If no column with the given index exists.
     */
    double doubleValue(int columnIndex);

    /**
     * Gets a {@code String} column value.
     *
     * @param columnName Column name in SQL-parser style notation; e.g., <br>
     *                   "myColumn" - "MYCOLUMN", returns the index of the column ignores case sensitivity, <br>
     *                   "\"MyColumn\"" - "MyColumn", returns the index of the column with respect to case sensitivity.
     * @return Column value.
     * @throws IllegalArgumentException If no column with the given name exists.
     */
    String stringValue(String columnName);

    /**
     * Gets a {@code String} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If no column with the given index exists.
     */
    String stringValue(int columnIndex);

    /**
     * Gets a {@code UUID} column value.
     *
     * @param columnName Column name in SQL-parser style notation; e.g., <br>
     *                   "myColumn" - "MYCOLUMN", returns the index of the column ignores case sensitivity, <br>
     *                   "\"MyColumn\"" - "MyColumn", returns the index of the column with respect to case sensitivity.
     * @return Column value.
     * @throws IllegalArgumentException If no column with the given name exists.
     */
    UUID uuidValue(String columnName);

    /**
     * Gets a {@code UUID} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If no column with the given index exists.
     */
    UUID uuidValue(int columnIndex);

    /**
     * Gets a {@code BitSet} column value.
     *
     * @param columnName Column name in SQL-parser style notation; e.g., <br>
     *                   "myColumn" - "MYCOLUMN", returns the index of the column ignores case sensitivity, <br>
     *                   "\"MyColumn\"" - "MyColumn", returns the index of the column with respect to case sensitivity.
     * @return Column value.
     * @throws IllegalArgumentException If no column with the given name exists.
     */
    BitSet bitmaskValue(String columnName);

    /**
     * Gets a {@code BitSet} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If no column with the given index exists.
     */
    BitSet bitmaskValue(int columnIndex);

    /**
     * Gets a {@code LocalDate} column value.
     *
     * @param columnName Column name in SQL-parser style notation; e.g., <br>
     *                   "myColumn" - "MYCOLUMN", returns the index of the column ignores case sensitivity, <br>
     *                   "\"MyColumn\"" - "MyColumn", returns the index of the column with respect to case sensitivity.
     * @return Column value.
     * @throws IllegalArgumentException If no column with the given name exists.
     */
    LocalDate dateValue(String columnName);

    /**
     * Gets a {@code LocalDate} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If no column with the given index exists.
     */
    LocalDate dateValue(int columnIndex);

    /**
     * Gets a {@code LocalTime} column value.
     *
     * @param columnName Column name in SQL-parser style notation; e.g., <br>
     *                   "myColumn" - "MYCOLUMN", returns the index of the column ignores case sensitivity, <br>
     *                   "\"MyColumn\"" - "MyColumn", returns the index of the column with respect to case sensitivity.
     * @return Column value.
     * @throws IllegalArgumentException If no column with the given name exists.
     */
    LocalTime timeValue(String columnName);

    /**
     * Gets a {@code LocalTime} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If no column with the given index exists.
     */
    LocalTime timeValue(int columnIndex);

    /**
     * Gets a {@code LocalDateTime} column value.
     *
     * @param columnName Column name in SQL-parser style notation; e.g., <br>
     *                   "myColumn" - "MYCOLUMN", returns the index of the column ignores case sensitivity, <br>
     *                   "\"MyColumn\"" - "MyColumn", returns the index of the column with respect to case sensitivity.
     * @return Column value.
     * @throws IllegalArgumentException If no column with the given name exists.
     */
    LocalDateTime datetimeValue(String columnName);

    /**
     * Gets a {@code LocalDateTime} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If no column with the iven index exists.
     */
    LocalDateTime datetimeValue(int columnIndex);

    /**
     * Gets a {@code Instant} column value.
     *
     * @param columnName Column name in SQL-parser style notation; e.g., <br>
     *                   "myColumn" - "MYCOLUMN", returns the index of the column ignores case sensitivity, <br>
     *                   "\"MyColumn\"" - "MyColumn", returns the index of the column with respect to case sensitivity.
     * @return Column value.
     * @throws IllegalArgumentException If no column with the given name exists.
     */
    Instant timestampValue(String columnName);

    /**
     * Gets a {@code Instant} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If no column with the given index exists.
     */
    Instant timestampValue(int columnIndex);

    /** {@inheritDoc} */
    @Override
    default Iterator<Object> iterator() {
        return new Iterator<>() {
            /** Current column index. */
            private int cur;

            /** {@inheritDoc} */
            @Override
            public boolean hasNext() {
                return cur < columnCount();
            }

            /** {@inheritDoc} */
            @Override
            public Object next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                return value(cur++);
            }
        };
    }
}
