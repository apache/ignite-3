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
import java.util.List;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataOutput;

/**
 * Catalog object data output.
 */
public class CatalogObjectDataOutput extends IgniteUnsafeDataOutput {

    /** Initial capacity (in bytes) of the buffer used for data output. */
    private static final int INITIAL_BUFFER_CAPACITY = 256;

    private final CatalogEntrySerializerProvider serializers;

    /**
     * Constructor.
     */
    public CatalogObjectDataOutput(CatalogEntrySerializerProvider serializers) {
        super(INITIAL_BUFFER_CAPACITY);

        this.serializers = serializers;
    }

    /**
     * Writes an entry with the latest serializer.
     *
     * @param entry Entry.
     */
    public void writeObject(MarshallableEntry entry) throws IOException {
        int typeId = entry.typeId();
        int entryVersion = serializers.latestSerializerVersion(typeId);

        writeObjectHeader(entry, entryVersion);
        serializers.get(entryVersion, typeId).writeTo(entry, this);
    }

    /**
     * Writes a list of object.
     *
     * @param objects Objects.
     */
    public <T extends MarshallableEntry> void writeObjects(List<T> objects) throws IOException {
        writeVarInt(objects.size());

        for (T object : objects) {
            int typeId = object.typeId();
            int version = serializers.latestSerializerVersion(typeId);

            writeObjectHeader(object, version);
            serializers.get(version, typeId).writeTo(object, this);
        }
    }

    /**
     * Writes a compact list of object.
     *
     * @param objects Objects.
     */
    public <T extends MarshallableEntry> void writeObjectsCompact(List<T> objects) throws IOException {
        if (objects.isEmpty()) {
            writeVarInt(0);
            return;
        }

        writeVarInt(objects.size());

        int typeId = objects.get(0).typeId();
        int version = serializers.latestSerializerVersion(typeId);
        writeObjectHeader(objects.get(0), version);

        CatalogObjectSerializer<MarshallableEntry> serializer = serializers.get(version, typeId);

        for (T object : objects) {
            serializer.writeTo(object, this);
        }
    }

    /**
     * Writes a list of elements.
     * <b>NOTE: To write versioned elements use {@link #writeObjects(List)} instead.</b>
     *
     * @param writer Element writer.
     * @param <T> Element type.
     */
    public <T> void writeList(ElementWriter<T> writer, List<T> list) throws IOException {
        writeVarInt(list.size());
        for (T element : list) {
            writer.write(this, element);
        }
    }

    private void writeObjectHeader(MarshallableEntry entry, int entryVersion) throws IOException {
        int typeId = entry.typeId();
        writeShort(typeId);
        writeVarInt(entryVersion);
    }

    /**
     * Element writer.
     *
     * @param <T> Element type.
     */
    @FunctionalInterface
    public interface ElementWriter<T> {
        /**
         * Writes the given element into the output.
         *
         * @param out Output.
         * @param element Element.
         */
        void write(CatalogObjectDataOutput out, T element) throws IOException;
    }
}
