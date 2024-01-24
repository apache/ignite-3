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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
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
    public static void writeStringCollection(Collection<String> list, DataOutput out) throws IOException {
        out.writeInt(list.size());

        for (String item : list) {
            out.writeUTF(item);
        }
    }

    /** Reads list of strings. */
    public static List<String> readStringList(DataInput in) throws IOException {
        int size = in.readInt();

        return readStringCollection(in, size, new ArrayList<>(size));
    }

    /** Reads set of strings. */
    public static Set<String> readStringSet(DataInput in) throws IOException {
        int size = in.readInt();

        return readStringCollection(in, size, new HashSet<>());
    }

    /** Reads array of objects. */
    public static <T> T[] readArray(int version, CatalogObjectSerializer<T> serializer, IgniteDataInput input, Class<T> clazz)
            throws IOException {
        int len = input.readInt();

        T[] arr = (T[]) Array.newInstance(clazz, len);

        for (int i = 0; i < len; i++) {
            arr[i] = serializer.readFrom(version, input);
        }

        return arr;
    }

    /** Writes array of objects. */
    public static <T> void writeArray(T[] items, int version, CatalogObjectSerializer<T> serializer, IgniteDataOutput output)
            throws IOException {
        output.writeInt(items.length);

        for (T item : items) {
            serializer.writeTo(item, version, output);
        }
    }

    /** Reads list of objects. */
    public static <T> List<T> readList(int version, CatalogObjectSerializer<T> serializer, IgniteDataInput input) throws IOException {
        int len = input.readInt();

        List<T> entries = new ArrayList<>(len);

        for (int i = 0; i < len; i++) {
            T item = serializer.readFrom(version, input);

            entries.add(item);
        }

        return entries;
    }

    /** Writes list of objects. */
    public static <T> void writeList(List<T> items, int version, CatalogObjectSerializer<T> serializer, IgniteDataOutput output)
            throws IOException {
        output.writeInt(items.size());

        for (T item : items) {
            serializer.writeTo(item, version, output);
        }
    }

    private static <T extends Collection<String>> T readStringCollection(DataInput input, int size, T collection) throws IOException {
        for (int i = 0; i < size; i++) {
            collection.add(input.readUTF());
        }

        return collection;
    }
}
