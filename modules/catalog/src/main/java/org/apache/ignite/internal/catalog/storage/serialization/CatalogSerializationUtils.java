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
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor.CatalogIndexDescriptorType;
import org.jetbrains.annotations.Nullable;

/**
 * Utility methods used for serializing catalog objects.
 */
public class CatalogSerializationUtils {
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
    public static void writeStringCollection(@Nullable Collection<String> list, CatalogObjectDataOutput out) throws IOException {
        if (list == null) {
            out.writeVarInt(-1);

            return;
        }

        out.writeVarInt(list.size());

        for (String item : list) {
            out.writeUTF(item);
        }
    }

    /** Reads collection containing strings. */
    public static <T extends Collection<String>> @Nullable T readStringCollection(CatalogObjectDataInput in, IntFunction<T> factory)
            throws IOException {
        int size = in.readVarIntAsInt();

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
    public static <T> T[] readArray(CatalogObjectSerializer<T> serializer, CatalogObjectDataInput input, Class<T> clazz)
            throws IOException {
        int len = input.readVarIntAsInt();

        T[] arr = (T[]) Array.newInstance(clazz, len);

        for (int i = 0; i < len; i++) {
            arr[i] = serializer.readFrom(input);
        }

        return arr;
    }

    /** Writes array of objects. */
    public static <T> void writeArray(T[] items, CatalogObjectSerializer<T> serializer, CatalogObjectDataOutput output)
            throws IOException {
        output.writeVarInt(items.length);

        for (T item : items) {
            serializer.writeTo(item, output);
        }
    }

    /** Reads list of objects. */
    public static <T> List<T> readList(CatalogObjectSerializer<T> serializer, CatalogObjectDataInput input)
            throws IOException {
        int len = input.readVarIntAsInt();

        List<T> entries = new ArrayList<>(len);

        for (int i = 0; i < len; i++) {
            T item = serializer.readFrom(input);

            entries.add(item);
        }

        return entries;
    }

    /** Reads nullable long. */
    public static @Nullable Long readNullableLong(DataInput in) throws IOException {
        if (!in.readBoolean()) {
            return null;
        }

        return in.readLong();
    }

    /** Reads nullable double. */
    public static @Nullable Double readNullableDouble(DataInput in) throws IOException {
        if (!in.readBoolean()) {
            return null;
        }

        return in.readDouble();
    }

    /** Writes nullable long. */
    public static void writeNullableLong(@Nullable Long value, DataOutput out) throws IOException {
        out.writeBoolean(value != null);

        if (value != null) {
            out.writeLong(value);
        }
    }

    /** Writes nullable long. */
    public static void writeNullableDouble(@Nullable Double value, DataOutput out) throws IOException {
        out.writeBoolean(value != null);

        if (value != null) {
            out.writeDouble(value);
        }
    }

    /** Writes list of objects. */
    public static <T> void writeList(List<T> items, CatalogObjectSerializer<T> serializer,
            CatalogObjectDataOutput output) throws IOException {
        output.writeVarInt(items.size());

        for (T item : items) {
            serializer.writeTo(item, output);
        }
    }

    /**
     * Base index descriptor serializer V1.
     *
     * @since 3.0.0
     * @deprecated Serializer is only used to detect index type, but type must be detected using {@link MarshallableEntry#typeId()}.
     */
    @Deprecated
    public static class IndexDescriptorSerializerHelper implements CatalogObjectSerializer<CatalogIndexDescriptor> {
        private final CatalogEntrySerializerProvider serializers;

        public IndexDescriptorSerializerHelper(CatalogEntrySerializerProvider serializers) {
            this.serializers = serializers;
        }

        @Override
        public CatalogIndexDescriptor readFrom(CatalogObjectDataInput input) throws IOException {
            int idxType = input.readByte();

            CatalogIndexDescriptorType type = CatalogIndexDescriptorType.forId(idxType);

            if (type == CatalogIndexDescriptorType.HASH) {
                return (CatalogIndexDescriptor) serializers.get(1, MarshallableEntryType.DESCRIPTOR_HASH_INDEX.id())
                        .readFrom(input);
            } else {
                return (CatalogIndexDescriptor) serializers.get(1, MarshallableEntryType.DESCRIPTOR_SORTED_INDEX.id())
                        .readFrom(input);
            }
        }

        @Override
        public void writeTo(CatalogIndexDescriptor descriptor, CatalogObjectDataOutput output) throws IOException {
            output.writeByte(descriptor.indexType().id());

            serializers.get(1, descriptor.typeId()).writeTo(descriptor, output);
        }
    }
}
