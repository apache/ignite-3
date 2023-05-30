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

package org.apache.ignite.internal.catalog.descriptors;


import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.tostring.S;

/**
 * Sorted index descriptor.
 */
public class SortedIndexDescriptor extends IndexDescriptor {
    private static final long serialVersionUID = 2085714310150728611L;

    private final List<IndexColumnDescriptor> columns;

    /**
     * Constructs a sorted description.
     *
     * @param id Id of the index.
     * @param name Name of the index.
     * @param tableId Id of the table index belongs to.
     * @param unique Unique flag.
     * @param columns A list of columns descriptors.
     * @throws IllegalArgumentException If columns list contains duplicates or columns size doesn't match the collations size.
     */
    public SortedIndexDescriptor(int id, String name, int tableId, boolean unique, List<IndexColumnDescriptor> columns) {
        super(id, name, tableId, unique);

        this.columns = Objects.requireNonNull(columns, "columns");
    }

    public List<IndexColumnDescriptor> columns() {
        return columns;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }
}


