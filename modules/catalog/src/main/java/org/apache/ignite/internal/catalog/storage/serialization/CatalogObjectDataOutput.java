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
     * Writes entry.
     */
    public void writeEntry(MarshallableEntry entry) throws IOException {
        int typeId = entry.typeId();
        int entryVersion = serializers.latestSerializerVersion(typeId);

        writeEntryHeader(entry, entryVersion);
        serializers.get(entryVersion, typeId).writeTo(entry, this);
    }

    /**
     * Writes entry list.
     */
    public void writeEntryList(List<? extends MarshallableEntry> entries) throws IOException {
        writeVarInt(entries.size());

        for (MarshallableEntry entry : entries) {
            int typeId = entry.typeId();
            int entryVersion = serializers.latestSerializerVersion(typeId);

            writeEntryHeader(entry, entryVersion);
            serializers.get(entryVersion, typeId).writeTo(entry, this);
        }
    }

    /**
     * Writes entry array.
     */
    public void writeEntryArray(MarshallableEntry[] entries) throws IOException {
        writeVarInt(entries.length);

        for (MarshallableEntry entry : entries) {
            int typeId = entry.typeId();
            int entryVersion = serializers.latestSerializerVersion(typeId);

            writeEntryHeader(entry, entryVersion);
            serializers.get(entryVersion, typeId).writeTo(entry, this);
        }
    }

    private void writeEntryHeader(MarshallableEntry entry, int entryVersion) throws IOException {
        int typeId = entry.typeId();
        writeShort(typeId);
        writeVarInt(entryVersion);
    }
}
