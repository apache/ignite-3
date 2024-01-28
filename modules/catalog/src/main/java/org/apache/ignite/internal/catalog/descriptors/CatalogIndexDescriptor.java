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

import static org.apache.ignite.internal.catalog.serialization.CatalogSerializationUtils.readList;
import static org.apache.ignite.internal.catalog.serialization.CatalogSerializationUtils.readStringCollection;
import static org.apache.ignite.internal.catalog.serialization.CatalogSerializationUtils.writeList;
import static org.apache.ignite.internal.catalog.serialization.CatalogSerializationUtils.writeStringCollection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor.CatalogDescriptorBaseSerializer.CatalogDescriptorBase;
import org.apache.ignite.internal.catalog.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/** Index descriptor base class. */
public abstract class CatalogIndexDescriptor extends CatalogObjectDescriptor {
    public static CatalogObjectSerializer<CatalogIndexDescriptor> SERIALIZER = new IndexDescriptorSerializer();

    /** Table ID. */
    private final int tableId;

    /** Unique constraint flag. */
    private final boolean unique;

    /** Index status. */
    private final CatalogIndexStatus status;

    CatalogIndexDescriptor(int id, String name, int tableId, boolean unique, CatalogIndexStatus status, long causalityToken) {
        super(id, Type.INDEX, name, causalityToken);
        this.tableId = tableId;
        this.unique = unique;
        this.status = Objects.requireNonNull(status, "status");
    }

    /** Gets table ID. */
    public int tableId() {
        return tableId;
    }

    /** Gets index unique flag. */
    public boolean unique() {
        return unique;
    }

    /** Returns index status. */
    public CatalogIndexStatus status() {
        return status;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }

    static class IndexDescriptorHeader {
        private final int tableId;
        private final boolean unique;
        private final CatalogIndexStatus status;

        IndexDescriptorHeader(int tableId, boolean unique, int statusId) {
            this.tableId = tableId;
            this.unique = unique;
            this.status = CatalogIndexStatus.forId(statusId);
        }

        IndexDescriptorHeader(CatalogIndexDescriptor indexDescriptor) {
            this.tableId = indexDescriptor.tableId();
            this.unique = indexDescriptor.unique();
            this.status = indexDescriptor.status();
        }
    }

    static class IndexDescriptorHeaderSerializer implements CatalogObjectSerializer<IndexDescriptorHeader> {
        static IndexDescriptorHeaderSerializer INSTANCE = new IndexDescriptorHeaderSerializer();

        @Override
        public IndexDescriptorHeader readFrom(int version, IgniteDataInput input) throws IOException {
            int tableId = input.readInt();
            boolean unique = input.readBoolean();
            int status = input.readByte();

            return new IndexDescriptorHeader(tableId, unique, status);
        }

        @Override
        public void writeTo(IndexDescriptorHeader header, int version, IgniteDataOutput output) throws IOException {
            output.writeInt(header.tableId);
            output.writeBoolean(header.unique);
            output.writeByte(header.status.id());
        }
    }

    private static class IndexDescriptorSerializer implements CatalogObjectSerializer<CatalogIndexDescriptor> {
        @Override
        public CatalogIndexDescriptor readFrom(int version, IgniteDataInput input) throws IOException {
            byte idxType = input.readByte();

            assert idxType == 0 || idxType == 1 : "Unknown index type: " + idxType;

            if (idxType == 0) {
                CatalogDescriptorBase header = CatalogObjectDescriptor.SERIALIZER.readFrom(version, input);
                IndexDescriptorHeader idxHeader = IndexDescriptorHeaderSerializer.INSTANCE.readFrom(version, input);
                List<CatalogIndexColumnDescriptor> columns = readList(version, CatalogIndexColumnDescriptor.SERIALIZER, input);

                return new CatalogSortedIndexDescriptor(
                        header.id(),
                        header.name(),
                        idxHeader.tableId,
                        idxHeader.unique,
                        columns,
                        idxHeader.status,
                        header.updateToken()
                );
            } else {
                CatalogDescriptorBase header = CatalogObjectDescriptor.SERIALIZER.readFrom(version, input);
                IndexDescriptorHeader idxHeader = IndexDescriptorHeaderSerializer.INSTANCE.readFrom(version, input);
                List<String> columns = readStringCollection(input, ArrayList::new);

                return new CatalogHashIndexDescriptor(
                        header.id(),
                        header.name(),
                        idxHeader.tableId,
                        idxHeader.unique,
                        columns,
                        idxHeader.status,
                        header.updateToken()
                );
            }
        }

        @Override
        public void writeTo(CatalogIndexDescriptor descriptor, int version, IgniteDataOutput output) throws IOException {
            if (descriptor instanceof CatalogSortedIndexDescriptor) {
                output.writeByte(0);

                CatalogObjectDescriptor.SERIALIZER.writeTo(new CatalogDescriptorBase(descriptor), version, output);
                IndexDescriptorHeaderSerializer.INSTANCE.writeTo(new IndexDescriptorHeader(descriptor), version, output);
                writeList(((CatalogSortedIndexDescriptor) descriptor).columns(), version, CatalogIndexColumnDescriptor.SERIALIZER, output);

            } else if (descriptor instanceof CatalogHashIndexDescriptor) {
                output.writeByte(1);

                CatalogObjectDescriptor.SERIALIZER.writeTo(new CatalogDescriptorBase(descriptor), version, output);
                IndexDescriptorHeaderSerializer.INSTANCE.writeTo(new IndexDescriptorHeader(descriptor), version, output);
                writeStringCollection(((CatalogHashIndexDescriptor) descriptor).columns(), output);
            } else {
                assert false : "Unknown index type: " + descriptor.getClass().getName();
            }
        }
    }
}
