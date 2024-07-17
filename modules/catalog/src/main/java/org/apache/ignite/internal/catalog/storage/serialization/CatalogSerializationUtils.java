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

package org.apache.ignite.internal.catalog.storage.serialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.IntFunction;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor.CatalogIndexDescriptorType;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.jetbrains.annotations.Nullable;

/**
 * Utility methods used for serializing catalog objects.
 */
public class CatalogSerializationUtils {
    public static final CatalogObjectSerializer<CatalogIndexDescriptor> IDX_SERIALIZER = new IndexDescriptorSerializer();

    /** Reads nullable string. */
    public static @Nullable String readNullableString(DataInput in) throws IOException {
        if (!in.readBoolean()) {
            return null;
        }

        return in.readUTF();
    }

    /** Writes nullable string. */
    public static void writeNullableString(@Nullable String str, DataOutput out) throws IOException {
        out.writeBoolean(str != null);

        if (str != null) {
            out.writeUTF(str);
        }
    }

    /** Writes collection containing strings. */
    public static void writeStringCollection(@Nullable Collection<String> list, DataOutput out) throws IOException {
        if (list == null) {
            out.writeInt(-1);

            return;
        }

        out.writeInt(list.size());

        for (String item : list) {
            out.writeUTF(item);
        }
    }

    /** Reads collection containing strings. */
    public static <T extends Collection<String>> @Nullable T readStringCollection(DataInput in, IntFunction<T> factory) throws IOException {
        int size = in.readInt();

        if (size == -1) {
            return null;
        }

        T collection = factory.apply(size);

        for (int i = 0; i < size; i++) {
            collection.add(in.readUTF());
        }

        return collection;
    }

    /** Reads array of objects. */
    public static <T> T[] readArray(CatalogObjectSerializer<T> serializer, IgniteDataInput input, Class<T> clazz)
            throws IOException {
        int len = input.readInt();

        T[] arr = (T[]) Array.newInstance(clazz, len);

        for (int i = 0; i < len; i++) {
            arr[i] = serializer.readFrom(input);
        }

        return arr;
    }

    /** Writes array of objects. */
    public static <T> void writeArray(T[] items, CatalogObjectSerializer<T> serializer, IgniteDataOutput output)
            throws IOException {
        output.writeInt(items.length);

        for (T item : items) {
            serializer.writeTo(item, output);
        }
    }

    /** Reads list of objects. */
    public static <T> List<T> readList(CatalogObjectSerializer<T> serializer, IgniteDataInput input) throws IOException {
        int len = input.readInt();

        List<T> entries = new ArrayList<>(len);

        for (int i = 0; i < len; i++) {
            T item = serializer.readFrom(input);

            entries.add(item);
        }

        return entries;
    }

    /** Writes list of objects. */
    public static <T> void writeList(List<T> items, CatalogObjectSerializer<T> serializer, IgniteDataOutput output)
            throws IOException {
        output.writeInt(items.size());

        for (T item : items) {
            serializer.writeTo(item, output);
        }
    }

    private static class IndexDescriptorSerializer implements CatalogObjectSerializer<CatalogIndexDescriptor> {
        @Override
        public CatalogIndexDescriptor readFrom(IgniteDataInput input) throws IOException {
            int idxType = input.readByte();

            CatalogIndexDescriptorType type = CatalogIndexDescriptorType.forId(idxType);

            if (type == CatalogIndexDescriptorType.HASH) {
                return CatalogHashIndexDescriptor.SERIALIZER.readFrom(input);
            } else {
                return CatalogSortedIndexDescriptor.SERIALIZER.readFrom(input);
            }
        }

        @Override
        public void writeTo(CatalogIndexDescriptor descriptor, IgniteDataOutput output) throws IOException {
            output.writeByte(descriptor.indexType().id());

            if (descriptor.indexType() == CatalogIndexDescriptorType.HASH) {
                CatalogHashIndexDescriptor.SERIALIZER.writeTo((CatalogHashIndexDescriptor) descriptor, output);
            } else {
                CatalogSortedIndexDescriptor.SERIALIZER.writeTo((CatalogSortedIndexDescriptor) descriptor, output);
            }
        }
    }
}
