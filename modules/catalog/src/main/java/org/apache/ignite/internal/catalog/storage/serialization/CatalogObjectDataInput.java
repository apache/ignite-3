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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
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
     * Reads entry.
     */
    public MarshallableEntry readEntry() throws IOException {
        int typeId = readShort();
        int entryVersion = readVarIntAsInt();

        return serializers.get(entryVersion, typeId).readFrom(this);
    }

    /**
     * Reads entry.
     */
    public <T extends MarshallableEntry> T readEntry(Class<T> type, int version) throws IOException {
        int typeId = readShort();
        int entryVersion = readVarIntAsInt();
        checkVersion(entryVersion, version, typeId);

        MarshallableEntry entry = serializers.get(entryVersion, typeId).readFrom(this);
        return type.cast(entry);
    }

    /**
     * Reads entry.
     */
    public <T extends MarshallableEntry> T readEntry(MarshallableEntryTypeInfo<T> type) throws IOException {
        int typeId = readShort();
        int entryVersion = readVarIntAsInt();
        int version = type.version(typeId);
        checkVersion(entryVersion, version, typeId);

        MarshallableEntry entry = serializers.get(entryVersion, typeId).readFrom(this);
        return type.castElement(entry);
    }

    /**
     * Reads entry list.
     */
    public <T extends MarshallableEntry> List<T> readEntryList(Class<T> type) throws IOException {
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
     * Reads entry list.
     *
     * @param type Type.
     * @see MarshallableEntryTypeInfo
     */
    public <T extends MarshallableEntry> List<T> readEntryList(MarshallableEntryTypeInfo<T> type) throws IOException {
        int size = readVarIntAsInt();
        List<T> list = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            int typeId = readShort();
            int entryVersion = readVarIntAsInt();
            int expectedVersion = type.version(typeId);
            checkVersion(entryVersion, expectedVersion, typeId);

            MarshallableEntry entry = serializers.get(entryVersion, typeId).readFrom(this);
            list.add(type.castElement(entry));
        }

        return list;
    }

    /**
     * Reads entry array.
     */
    public <T extends MarshallableEntry> T[] readEntryArray(Class<T> type) throws IOException {
        int size = readVarIntAsInt();
        T[] array = (T[]) Array.newInstance(type, size);

        for (int i = 0; i < size; i++) {
            int typeId = readShort();
            int entryVersion = readVarIntAsInt();

            MarshallableEntry entry = serializers.get(entryVersion, typeId).readFrom(this);
            array[i] = (type.cast(entry));
        }

        return array;
    }

    /**
     * Reads entry array.
     *
     * @param type Type.
     * @see MarshallableEntryTypeInfo
     */
    public <T extends MarshallableEntry> T[] readEntryArray(MarshallableEntryTypeInfo<T> type) throws IOException {
        int size = readVarIntAsInt();
        T[] array = (T[]) Array.newInstance(type.type(), size);

        for (int i = 0; i < size; i++) {
            int typeId = readShort();
            int entryVersion = readVarIntAsInt();
            int expectedVersion = type.version(typeId);
            checkVersion(entryVersion, expectedVersion, typeId);

            MarshallableEntry entry = serializers.get(entryVersion, typeId).readFrom(this);
            array[i] = (type.castElement(entry));
        }

        return array;
    }

    private static void checkVersion(int entryVersion, int expectedVersion, int typeId) {
        assert entryVersion == expectedVersion : format(
                "Versions do not match. TypeId: {}, expected by serializer: {}, read from output: {}",
                typeId, expectedVersion, entryVersion
        );
    }
}
