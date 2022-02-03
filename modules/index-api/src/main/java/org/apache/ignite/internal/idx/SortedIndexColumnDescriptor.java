/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.idx;

import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.tostring.S;

/**
 * Descriptor of a Sorted Index column (column name and column sort order).
 */
public class SortedIndexColumnDescriptor {
    private final Column column;

    private final SortedIndexColumnCollation collation;

    public SortedIndexColumnDescriptor(Column column, boolean asc) {
        this(column, new SortedIndexColumnCollation(asc));
    }

    public SortedIndexColumnDescriptor(Column column, SortedIndexColumnCollation collation) {
        this.column = column;
        this.collation = collation;
    }

    /**
     * Returns a column descriptor.
     */
    public Column column() {
        return column;
    }

    /**
     * Returns column collation.
     */
    public SortedIndexColumnCollation collation() {
        return collation;
    }

    /**
     * Returns {@code true} if this column is sorted in ascending order or {@code false} otherwise.
     */
    public boolean asc() {
        return collation.asc();
    }

    /**
     * Returns {@code true} if this column can contain null values or {@code false} otherwise.
     */
    public boolean nullable() {
        return column.nullable();
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
