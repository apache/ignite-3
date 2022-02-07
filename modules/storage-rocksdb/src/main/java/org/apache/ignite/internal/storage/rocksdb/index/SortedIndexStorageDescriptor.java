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

package org.apache.ignite.internal.storage.rocksdb.index;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.idx.SortedIndexColumnDescriptor;
import org.apache.ignite.internal.idx.SortedIndexDescriptor;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.storage.index.IndexSchemaDescriptor;

/**
 * Descriptor for creating a Sorted Index Storage.
 * TODO: IGNITE-16105 Replace sorted index binary storage protocol
 */
public class SortedIndexStorageDescriptor extends SortedIndexDescriptor {
    private final List<SortedIndexColumnDescriptor> idxColumns;

    private final IndexSchemaDescriptor idxSchema;

    /**
     * Creates an Index Descriptor from a given Table Configuration.
     */
    public SortedIndexStorageDescriptor(SortedIndexDescriptor desc) {
        super(desc.name(), desc.columns(), desc.pkColumns());

        this.idxSchema = new IndexSchemaDescriptor(
                IntStream.range(0, columns().size())
                        .mapToObj(i -> columns().get(i).column().copyWithOrder(i))
                        .toArray(Column[]::new)
        );

        idxColumns = Arrays.asList(idxSchema.columns()).stream()
                .sorted(Comparator.comparing(Column::columnOrder))
                .map(c -> {
                    SortedIndexColumnDescriptor origDesc = columns().stream()
                            .filter(origCol -> origCol.column().name().equals(c.name()))
                            .findAny()
                            .get();

                    return new SortedIndexColumnDescriptor(c, origDesc.collation());
                })
                .collect(Collectors.toList());
    }

    /**
     * Converts this Descriptor into an equivalent {@link SchemaDescriptor}.
     * TODO: IGNITE-16105 Replace sorted index binary storage protocol
     *
     * <p>The resulting {@code SchemaDescriptor} will have empty {@link SchemaDescriptor#valueColumns()} and its
     * {@link SchemaDescriptor#keyColumns()} will be consistent with the columns returned by {@link #columns()}.
     */
    public SchemaDescriptor schema() {
        return idxSchema;
    }

    /**
     * Index columns with {@link Column#schemaIndex()} specified for index storage schema.
     * TODO: IGNITE-16105 Replace sorted index binary storage protocol
     */
    public List<SortedIndexColumnDescriptor> indexColumns() {
        return idxColumns;
    }
}
