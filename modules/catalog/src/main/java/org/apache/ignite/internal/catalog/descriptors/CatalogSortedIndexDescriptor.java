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

/** Sorted index descriptor. */
public class CatalogSortedIndexDescriptor extends CatalogIndexDescriptor {
    private static final long serialVersionUID = 2085714310150728611L;

    private final List<CatalogIndexColumnDescriptor> columns;

    /**
     * Constructs a sorted index descriptor in write-only state.
     *
     * @param id Id of the index.
     * @param name Name of the index.
     * @param tableId Id of the table index belongs to.
     * @param unique Unique flag.
     * @param columns A list of columns descriptors.
     * @throws IllegalArgumentException If columns list contains duplicates or columns size doesn't match the collations size.
     */
    public CatalogSortedIndexDescriptor(
            int id,
            String name,
            int tableId,
            boolean unique,
            List<CatalogIndexColumnDescriptor> columns
    ) {
        this(id, name, tableId, unique, columns, false);
    }

    /**
     * Constructs a sorted index descriptor.
     *
     * @param id Id of the index.
     * @param name Name of the index.
     * @param tableId Id of the table index belongs to.
     * @param unique Unique flag.
     * @param columns A list of columns descriptors.
     * @param available Availability flag, {@code true} means it is available (the index has been built), otherwise it is registered
     *      (the index has not yet been built).
     * @throws IllegalArgumentException If columns list contains duplicates or columns size doesn't match the collations size.
     */
    public CatalogSortedIndexDescriptor(
            int id,
            String name,
            int tableId,
            boolean unique,
            List<CatalogIndexColumnDescriptor> columns,
            boolean available
    ) {
        super(id, name, tableId, unique, available);

        this.columns = Objects.requireNonNull(columns, "columns");
    }

    /** Returns indexed columns. */
    public List<CatalogIndexColumnDescriptor> columns() {
        return columns;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}


