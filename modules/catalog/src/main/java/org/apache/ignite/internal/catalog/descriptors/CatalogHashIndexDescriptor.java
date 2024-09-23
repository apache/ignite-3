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
import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.readStringCollection;
import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.writeStringCollection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/** Hash index descriptor. */
public class CatalogHashIndexDescriptor extends CatalogIndexDescriptor {
    public static final CatalogObjectSerializer<CatalogHashIndexDescriptor> SERIALIZER = new HashIndexDescriptorSerializer();

    private final List<String> columns;

    /**
     * Constructs a hash index descriptor in status {@link CatalogIndexStatus#REGISTERED}.
     *
     * @param id Id of the index.
     * @param name Name of the index.
     * @param tableId Id of the table index belongs to.
     * @param unique Unique flag.
     * @param columns A list of indexed columns. Must not contains duplicates.
     *
     * @throws IllegalArgumentException If columns list contains duplicates.
     */
    public CatalogHashIndexDescriptor(int id, String name, int tableId, boolean unique, List<String> columns) {
        this(id, name, tableId, unique, CatalogIndexStatus.REGISTERED, columns, INITIAL_CAUSALITY_TOKEN, false);
    }

    /**
     * Constructs a hash index descriptor.
     *
     * @param id Id of the index.
     * @param name Name of the index.
     * @param tableId Id of the table index belongs to.
     * @param unique Unique flag.
     * @param status Index status.
     * @param columns A list of indexed columns. Must not contains duplicates.
     * @param isCreatedWithTable Flag indicating that this index has been created at the same time as its table.
     *
     * @throws IllegalArgumentException If columns list contains duplicates.
     */
    public CatalogHashIndexDescriptor(
            int id,
            String name,
            int tableId,
            boolean unique,
            CatalogIndexStatus status,
            List<String> columns,
            boolean isCreatedWithTable
    ) {
        this(id, name, tableId, unique, status, columns, INITIAL_CAUSALITY_TOKEN, isCreatedWithTable);
    }

    /**
     * Constructs a hash index descriptor.
     *
     * @param id Id of the index.
     * @param name Name of the index.
     * @param tableId Id of the table index belongs to.
     * @param unique Unique flag.
     * @param status Index status.
     * @param columns A list of indexed columns. Must not contains duplicates.
     * @param causalityToken Token of the update of the descriptor.
     * @param isCreatedWithTable Flag indicating that this index has been created at the same time as its table.
     *
     * @throws IllegalArgumentException If columns list contains duplicates.
     */
    private CatalogHashIndexDescriptor(
            int id,
            String name,
            int tableId,
            boolean unique,
            CatalogIndexStatus status,
            List<String> columns,
            long causalityToken,
            boolean isCreatedWithTable
    ) {
        super(CatalogIndexDescriptorType.HASH, id, name, tableId, unique, status, causalityToken, isCreatedWithTable);

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

    private static class HashIndexDescriptorSerializer implements CatalogObjectSerializer<CatalogHashIndexDescriptor> {
        @Override
        public CatalogHashIndexDescriptor readFrom(IgniteDataInput input) throws IOException {
            int id = input.readInt();
            String name = input.readUTF();
            long updateToken = input.readLong();
            int tableId = input.readInt();
            boolean unique = input.readBoolean();
            CatalogIndexStatus status = CatalogIndexStatus.forId(input.readByte());
            boolean isCreatedWithTable = input.readBoolean();
            List<String> columns = readStringCollection(input, ArrayList::new);

            return new CatalogHashIndexDescriptor(id, name, tableId, unique, status, columns, updateToken, isCreatedWithTable);
        }

        @Override
        public void writeTo(CatalogHashIndexDescriptor descriptor, IgniteDataOutput output) throws IOException {
            output.writeInt(descriptor.id());
            output.writeUTF(descriptor.name());
            output.writeLong(descriptor.updateToken());
            output.writeInt(descriptor.tableId());
            output.writeBoolean(descriptor.unique());
            output.writeByte(descriptor.status().id());
            output.writeBoolean(descriptor.isCreatedWithTable());
            writeStringCollection(descriptor.columns(), output);
        }
    }
}
