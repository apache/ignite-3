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


import static org.apache.ignite.internal.catalog.CatalogManagerImpl.INITIAL_CAUSALITY_TOKEN;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.REGISTERED;
import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.readList;
import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.writeList;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/** Sorted index descriptor. */
public class CatalogSortedIndexDescriptor extends CatalogIndexDescriptor {
    public static final CatalogObjectSerializer<CatalogSortedIndexDescriptor> SERIALIZER = new SortedIndexDescriptorSerializer();

    private final List<CatalogIndexColumnDescriptor> columns;

    /**
     * Constructs a sorted index descriptor in status {@link CatalogIndexStatus#REGISTERED}.
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
        this(id, name, tableId, unique, REGISTERED, columns);
    }

    /**
     * Constructs a sorted index descriptor.
     *
     * @param id Id of the index.
     * @param name Name of the index.
     * @param tableId Id of the table index belongs to.
     * @param unique Unique flag.
     * @param status Index status.
     * @param columns A list of columns descriptors.
     * @throws IllegalArgumentException If columns list contains duplicates or columns size doesn't match the collations size.
     */
    public CatalogSortedIndexDescriptor(
            int id,
            String name,
            int tableId,
            boolean unique,
            CatalogIndexStatus status,
            List<CatalogIndexColumnDescriptor> columns
    ) {
        this(id, name, tableId, unique, status, columns, INITIAL_CAUSALITY_TOKEN);
    }

    /**
     * Constructs a sorted index descriptor.
     *
     * @param id Id of the index.
     * @param name Name of the index.
     * @param tableId Id of the table index belongs to.
     * @param unique Unique flag.
     * @param status Index status.
     * @param columns A list of columns descriptors.
     * @param causalityToken Token of the update of the descriptor.
     * @throws IllegalArgumentException If columns list contains duplicates or columns size doesn't match the collations size.
     */
    private CatalogSortedIndexDescriptor(
            int id,
            String name,
            int tableId,
            boolean unique,
            CatalogIndexStatus status,
            List<CatalogIndexColumnDescriptor> columns,
            long causalityToken
    ) {
        super(CatalogIndexDescriptorType.SORTED, id, name, tableId, unique, status, causalityToken);

        this.columns = Objects.requireNonNull(columns, "columns");
    }

    /** Returns indexed columns. */
    public List<CatalogIndexColumnDescriptor> columns() {
        return columns;
    }

    @Override
    public String toString() {
        return S.toString(CatalogSortedIndexDescriptor.class, this, super.toString());
    }

    private static class SortedIndexDescriptorSerializer implements CatalogObjectSerializer<CatalogSortedIndexDescriptor> {
        @Override
        public CatalogSortedIndexDescriptor readFrom(IgniteDataInput input) throws IOException {
            int id = input.readInt();
            String name = input.readUTF();
            long updateToken = input.readLong();
            int tableId = input.readInt();
            boolean unique = input.readBoolean();
            CatalogIndexStatus status = CatalogIndexStatus.forId(input.readByte());
            List<CatalogIndexColumnDescriptor> columns = readList(CatalogIndexColumnDescriptor.SERIALIZER, input);

            return new CatalogSortedIndexDescriptor(id, name, tableId, unique, status, columns, updateToken);
        }

        @Override
        public void writeTo(CatalogSortedIndexDescriptor descriptor, IgniteDataOutput output) throws IOException {
            output.writeInt(descriptor.id());
            output.writeUTF(descriptor.name());
            output.writeLong(descriptor.updateToken());
            output.writeInt(descriptor.tableId());
            output.writeBoolean(descriptor.unique());
            output.writeByte(descriptor.status().id());
            writeList(descriptor.columns(), CatalogIndexColumnDescriptor.SERIALIZER, output);
        }
    }
}
