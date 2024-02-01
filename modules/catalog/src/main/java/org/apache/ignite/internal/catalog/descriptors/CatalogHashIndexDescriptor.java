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

import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.REGISTERED;

import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.tostring.S;

/** Hash index descriptor. */
public class CatalogHashIndexDescriptor extends CatalogIndexDescriptor {
    private static final long serialVersionUID = -6784028115063219759L;

    private final List<String> columns;

    /**
     * Constructs a hash index descriptor in status {@link CatalogIndexStatus#REGISTERED}.
     *
     * @param id Id of the index.
     * @param name Name of the index.
     * @param tableId Id of the table index belongs to.
     * @param unique Unique flag.
     * @param creationCatalogVersion Catalog version in which the index was created.
     * @param columns A list of indexed columns. Must not contains duplicates.
     * @throws IllegalArgumentException If columns list contains duplicates.
     */
    public CatalogHashIndexDescriptor(int id, String name, int tableId, boolean unique, int creationCatalogVersion, List<String> columns) {
        this(id, name, tableId, unique, REGISTERED, creationCatalogVersion, columns);
    }

    /**
     * Constructs a hash index descriptor.
     *
     * @param id Id of the index.
     * @param name Name of the index.
     * @param tableId Id of the table index belongs to.
     * @param unique Unique flag.
     * @param status Index status.
     * @param creationCatalogVersion Catalog version in which the index was created.
     * @param columns A list of indexed columns. Must not contains duplicates.
     * @throws IllegalArgumentException If columns list contains duplicates.
     */
    public CatalogHashIndexDescriptor(
            int id,
            String name,
            int tableId,
            boolean unique,
            CatalogIndexStatus status,
            int creationCatalogVersion,
            List<String> columns
    ) {
        super(id, name, tableId, unique, status, creationCatalogVersion);

        this.columns = List.copyOf(Objects.requireNonNull(columns, "columns"));
    }

    /** Returns indexed columns. */
    public List<String> columns() {
        return columns;
    }

    @Override
    public String toString() {
        return S.toString(CatalogHashIndexDescriptor.class, this, super.toString());
    }
}
