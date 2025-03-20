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
import java.util.Collection;
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
    public void writeEntry(MarshallableEntry entry) throws IOException {
        int typeId = entry.typeId();
        int entryVersion = serializers.latestSerializerVersion(typeId);

        writeEntryHeader(entry, entryVersion);
        serializers.get(entryVersion, typeId).writeTo(entry, this);
    }

    /**
     * Writes a list of entries.
     *
     * @param entries Entries.
     */
    public <T extends MarshallableEntry> void writeEntryList(List<T> entries) throws IOException {
        writeVarInt(entries.size());

        for (T entry : entries) {
            int typeId = entry.typeId();
            int version = serializers.latestSerializerVersion(typeId);

            writeEntryHeader(entry, version);
            serializers.get(version, typeId).writeTo(entry, this);
        }
    }

    /**
     * Writes a list of entries of exactly the same type in a compact way.
     *
     * @param entries Entries.
     */
    public <T extends MarshallableEntry> void writeCompactEntryList(List<T> entries) throws IOException {
        if (entries.isEmpty()) {
            writeVarInt(0);
            return;
        }

        writeVarInt(entries.size());

        int typeId = entries.get(0).typeId();
        int version = serializers.latestSerializerVersion(typeId);
        writeEntryHeader(entries.get(0), version);

        CatalogObjectSerializer<MarshallableEntry> serializer = serializers.get(version, typeId);

        for (T entry : entries) {
            assert entry.typeId() == typeId : "Entry type do not match";
            serializer.writeTo(entry, this);
        }
    }

    /**
     * Writes a collection of non-versioned objects.
     * <b>NOTE: To write versioned elements use {@link #writeEntryList(List)} or {@link #writeCompactEntryList(List)} instead.</b>
     *
     * @param writer Element writer.
     * @param <T> Element type.
     */
    public <T> void writeObjectCollection(ElementWriter<T> writer, Collection<T> list) throws IOException {
        writeVarInt(list.size());
        for (T element : list) {
            writer.write(this, element);
        }
    }

    private void writeEntryHeader(MarshallableEntry entry, int entryVersion) throws IOException {
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
