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

package org.apache.ignite.internal.catalog.serialization;

import static org.apache.ignite.internal.catalog.serialization.CatalogSerializationUtils.readList;
import static org.apache.ignite.internal.catalog.serialization.CatalogSerializationUtils.readStringList;
import static org.apache.ignite.internal.catalog.serialization.CatalogSerializationUtils.writeList;
import static org.apache.ignite.internal.catalog.serialization.CatalogSerializationUtils.writeStringCollection;

import java.io.IOException;
import java.util.List;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.serialization.DescriptorHeaderSerializer.CatalogDescriptorHeader;
import org.apache.ignite.internal.catalog.storage.NewIndexEntry;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Serializer for {@link NewIndexEntry}.
 */
@SuppressWarnings("InnerClassFieldHidesOuterClassField")
class NewIndexEntrySerializer implements CatalogEntrySerializer<NewIndexEntry> {
    static NewIndexEntrySerializer INSTANCE = new NewIndexEntrySerializer();

    @Override
    public NewIndexEntry readFrom(int version, IgniteDataInput input) throws IOException {
        String schemaName = input.readUTF();
        CatalogIndexDescriptor descriptor = IndexDescriptorSerializer.INSTANCE.readFrom(version, input);

        return new NewIndexEntry(descriptor, schemaName);
    }

    @Override
    public void writeTo(NewIndexEntry entry, int version, IgniteDataOutput output) throws IOException {
        output.writeUTF(entry.schemaName());
        IndexDescriptorSerializer.INSTANCE.writeTo(entry.descriptor(), version, output);
    }

    static class IndexDescriptorHeader {
        private final int tableId;
        private final boolean unique;
        private final CatalogIndexStatus status;

        IndexDescriptorHeader(int tableId, boolean unique, int statusId) {
            this.tableId = tableId;
            this.unique = unique;
            this.status = CatalogIndexStatus.getById(statusId);
        }

        IndexDescriptorHeader(CatalogIndexDescriptor indexDescriptor) {
            this.tableId = indexDescriptor.tableId();
            this.unique = indexDescriptor.unique();
            this.status = indexDescriptor.status();
        }
    }

    static class IndexDescriptorHeaderSerializer implements CatalogEntrySerializer<IndexDescriptorHeader> {
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

    static class IndexDescriptorSerializer implements CatalogEntrySerializer<CatalogIndexDescriptor> {
        static IndexDescriptorSerializer INSTANCE = new IndexDescriptorSerializer();

        @Override
        public CatalogIndexDescriptor readFrom(int version, IgniteDataInput input) throws IOException {
            byte idxType = input.readByte();

            assert idxType == 0 || idxType == 1 : "Unknown index type";

            if (idxType == 0) {
                CatalogDescriptorHeader header = DescriptorHeaderSerializer.INSTANCE.readFrom(version, input);
                IndexDescriptorHeader idxHeader = IndexDescriptorHeaderSerializer.INSTANCE.readFrom(version, input);
                List<CatalogIndexColumnDescriptor> columns = readList(version, input, IndexColumnDescriptorSerializer.INSTANCE);

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
                CatalogDescriptorHeader header = DescriptorHeaderSerializer.INSTANCE.readFrom(version, input);
                IndexDescriptorHeader idxHeader = IndexDescriptorHeaderSerializer.INSTANCE.readFrom(version, input);
                List<String> columns = readStringList(input);

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

                DescriptorHeaderSerializer.INSTANCE.writeTo(new CatalogDescriptorHeader(descriptor), version, output);
                IndexDescriptorHeaderSerializer.INSTANCE.writeTo(new IndexDescriptorHeader(descriptor), version, output);
                writeList(((CatalogSortedIndexDescriptor) descriptor).columns(), version, IndexColumnDescriptorSerializer.INSTANCE, output);

            } else if (descriptor instanceof CatalogHashIndexDescriptor) {
                output.writeByte(1);

                DescriptorHeaderSerializer.INSTANCE.writeTo(new CatalogDescriptorHeader(descriptor), version, output);
                IndexDescriptorHeaderSerializer.INSTANCE.writeTo(new IndexDescriptorHeader(descriptor), version, output);
                writeStringCollection(((CatalogHashIndexDescriptor) descriptor).columns(), output);
            } else {
                assert false : "Unknown index type: " + descriptor.getClass().getName();
            }
        }
    }

    static class IndexColumnDescriptorSerializer implements CatalogEntrySerializer<CatalogIndexColumnDescriptor> {
        static IndexColumnDescriptorSerializer INSTANCE = new IndexColumnDescriptorSerializer();

        @Override
        public CatalogIndexColumnDescriptor readFrom(int version, IgniteDataInput input) throws IOException {
            String name = input.readUTF();
            boolean asc = input.readBoolean();
            boolean nullsFirst = input.readBoolean();

            CatalogColumnCollation collation = CatalogColumnCollation.get(asc, nullsFirst);

            return new CatalogIndexColumnDescriptor(name, collation);
        }

        @Override
        public void writeTo(CatalogIndexColumnDescriptor descriptor, int version, IgniteDataOutput output) throws IOException {
            output.writeUTF(descriptor.name());
            output.writeBoolean(descriptor.collation().asc());
            output.writeBoolean(descriptor.collation().nullsFirst());
        }
    }
}
