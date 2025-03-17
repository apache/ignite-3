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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataInput;

/**
 * Catalog object data input.
 */
public class CatalogObjectDataInput extends IgniteUnsafeDataInput {

    private final CatalogEntrySerializerProvider serializers;

    /**
     * Constructor.
     */
    public CatalogObjectDataInput(CatalogEntrySerializerProvider serializers, byte[] bytes) {
        super(bytes);

        this.serializers = serializers;
    }

    /**
     * Reads an object using a serializer of the version it was written with.
     */
    public MarshallableEntry readObject() throws IOException {
        int typeId = readShort();
        int entryVersion = readVarIntAsInt();

        return serializers.get(entryVersion, typeId).readFrom(this);
    }

    /**
     * Reads an object.
     *
     * @param type Object type.
     */
    public <T extends MarshallableEntry> T readObject(Class<T> type) throws IOException {
        int typeId = readShort();
        int entryVersion = readVarIntAsInt();
        MarshallableEntry entry = serializers.get(entryVersion, typeId).readFrom(this);
        return type.cast(entry);
    }

    /**
     * Reads an object list.
     *
     * @param type Type.
     */
    public <T extends MarshallableEntry> List<T> readObjects(Class<T> type) throws IOException {
        int size = readVarIntAsInt();
        List<T> list = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            int typeId = readShort();
            int entryVersion = readVarIntAsInt();

            MarshallableEntry entry = serializers.get(entryVersion, typeId).readFrom(this);
            list.add(type.cast(entry));
        }

        return list;
    }

    /**
     * Reads a compact object list.
     *
     * @param type Type.
     */
    public <T extends MarshallableEntry> List<T> readObjectsCompact(Class<T> type) throws IOException {
        int size = readVarIntAsInt();
        if (size == 0) {
            return Collections.emptyList();
        }

        List<T> list = new ArrayList<>(size);
        int typeId = readShort();
        int entryVersion = readVarIntAsInt();
        CatalogObjectSerializer<MarshallableEntry> serializer = serializers.get(entryVersion, typeId);

        for (int i = 0; i < size; i++) {
            MarshallableEntry entry = serializer.readFrom(this);
            list.add(type.cast(entry));
        }

        return list;
    }

    /**
     * Reads a list of elements.
     * <b>NOTE: To read versioned elements use {@link #readObjects(Class)} or {@link #readObjectsCompact(Class)} instead.</b>
     *
     * @param reader Element reader.
     * @param <T> Element type.
     *
     * @return A list of elements.
     */
    public <T> List<T> readList(ElementReader<T> reader) throws IOException {
        int size = readVarIntAsInt();
        List<T> list = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            T element = reader.read(this);
            list.add(element);
        }

        return list;
    }

    /**
     * Element reader.
     *
     * @param <T> Element type.
     */
    @FunctionalInterface
    public interface ElementReader<T> {
        /**
         * Reads an element from the input.
         *
         * @param in Input.
         */
        T read(CatalogObjectDataInput in) throws IOException;
    }
}
