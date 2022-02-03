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

import java.util.List;
import org.apache.ignite.internal.schema.Column;

/**
 * Descriptor for creating a Sorted Index Storage.
 */
public class SortedIndexDescriptor {
    private final String name;

    private final List<SortedIndexColumnDescriptor> columns;

    private final Column[] pkColumns;

    /**
     * Creates an Index Descriptor from a given Table Configuration.
     *
     * @param name        Index name.
     * @param columns     Index's columns.
     */
    public SortedIndexDescriptor(
            String name,
            final List<SortedIndexColumnDescriptor> columns,
            Column[] pkColumns) {
        this.name = name;
        this.pkColumns = pkColumns;

        this.columns = columns;
    }

    /**
     * Returns this index' name.
     */
    public String name() {
        return name;
    }

    /**
     * Returns the Column Descriptors that comprise a row of this index (indexed columns + primary key columns).
     */
    public List<SortedIndexColumnDescriptor> columns() {
        return columns;
    }

    /**
     * Returns the Column Descriptors that comprise a row of this index (indexed columns + primary key columns).
     */
    public Column[] pkColumns() {
        return pkColumns;
    }
}
