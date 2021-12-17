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

package org.apache.ignite.internal.storage.index;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;

/**
 * Descriptor for creating a Sorted Index Storage.
 */
public class SortedIndexDescriptor {
    private final String name;

    private final List<SortedIndexColumnDescriptor> columns;

    private final IndexSchemaDescriptor idxSchema;

    /**
     * Creates an Index Descriptor from a given Table Configuration.
     *
     * @param name        Index name.
     * @param columns     Index's columns.
     */
    public SortedIndexDescriptor(String name, final List<SortedIndexColumnDescriptor> columns) {
        this.name = name;

        this.idxSchema = new IndexSchemaDescriptor(
                IntStream.range(0, columns.size())
                        .mapToObj(i -> columns.get(i).column().copyWithOrder(i))
                        .toArray(Column[]::new)
        );

        this.columns = Arrays.asList(idxSchema.columns()).stream()
                .sorted(Comparator.comparing(Column::columnOrder))
                .map(c -> {
                    SortedIndexColumnDescriptor origDesc = columns.stream()
                            .filter(origCol -> origCol.column().name().equals(c.name()))
                            .findAny()
                            .get();

                    return new SortedIndexColumnDescriptor(c, origDesc.collation());
                })
                .collect(Collectors.toList());
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
