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
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.tostring.S;

/**
 * Descriptor for creating a Sorted Index Storage.
 *
 */
public class MySortedIndexDescriptor {
    private final String name;

    private final List<SortedIndexColumnDescriptor> columns;

    private final SchemaDescriptor idxSchema;

    /**
     * Creates an Index Descriptor from a given Table Configuration.
     *
     * @param name        Index name.
     * @param columns     Index's columns.
     */
    public MySortedIndexDescriptor(String name, List<SortedIndexColumnDescriptor> columns) {
        this.name = name;
        this.columns = columns;
        this.idxSchema = new SchemaDescriptor(
                0,
                columns.stream().map(SortedIndexColumnDescriptor::column).toArray(Column[]::new),
                new Column[0]
        );
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
     * Converts this Descriptor into an equivalent {@link SchemaDescriptor}.
     *
     * <p>The resulting {@code SchemaDescriptor} will have empty {@link SchemaDescriptor#valueColumns()} and its
     * {@link SchemaDescriptor#keyColumns()} will be consistent with the columns returned by {@link #indexRowColumns()}.
     */
    public SchemaDescriptor schema() {
        return idxSchema;
    }
}
