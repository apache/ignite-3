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

package org.apache.ignite.catalog;

import java.util.Objects;

/**
 * Column reference with sort order, used in indexes or primary keys.
 */
public final class ColumnSorted {
    private final String columnName;

    private SortOrder sortOrder = SortOrder.DEFAULT;

    private ColumnSorted(String columnName) {
        Objects.requireNonNull(columnName, "Index column name must not be null");

        this.columnName = columnName;
    }

    /**
     * Creates a reference to the column with the given name and default sort order.
     *
     * @param name Column name.
     * @return Created reference.
     */
    public static ColumnSorted column(String name) {
        return new ColumnSorted(name);
    }

    /**
     * Creates a reference to the column with the given name and sort order.
     *
     * @param name Column name.
     * @param sortOrder Sort order.
     * @return Created reference.
     */
    public static ColumnSorted column(String name, SortOrder sortOrder) {
        return new ColumnSorted(name).sort(sortOrder);
    }

    /**
     * Assigns ascending sort order.
     *
     * @return This object.
     */
    public ColumnSorted asc() {
        return sort(SortOrder.ASC);
    }

    /**
     * Assigns descending sort order.
     *
     * @return This object.
     */
    public ColumnSorted desc() {
        return sort(SortOrder.DESC);
    }

    /**
     * Assigns specified order.
     *
     * @param sortOrder Sort order.
     * @return This object.
     */
    public ColumnSorted sort(SortOrder sortOrder) {
        this.sortOrder = sortOrder;
        return this;
    }

    /**
     * Returns column name.
     *
     * @return Column name.
     */
    public String columnName() {
        return columnName;
    }

    /**
     * Returns sort order.
     *
     * @return sort order.
     */
    public SortOrder sortOrder() {
        return sortOrder;
    }

    @Override
    public String toString() {
        return "ColumnSorted{"
                + "columnName='" + columnName + '\''
                + ", sortOrder=" + sortOrder + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnSorted that = (ColumnSorted) o;
        return Objects.equals(columnName, that.columnName) && sortOrder == that.sortOrder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, sortOrder);
    }
}
