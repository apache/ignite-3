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

package org.apache.ignite.internal.catalog.storage;

import java.io.IOException;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataInput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataOutput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializer;

/**
 * Serializers for {@link DropTableEntry}.
 */
public class DropTableEntrySerializers {
    /**
     * Serializer for {@link DropTableEntry}.
     */
    @CatalogSerializer(version = 1, since = "3.0.0")
    static class DropTableEntrySerializerV1 implements CatalogObjectSerializer<DropTableEntry> {
        @Override
        public DropTableEntry readFrom(CatalogObjectDataInput input)throws IOException {
            int tableId = input.readVarIntAsInt();

            return new DropTableEntry(tableId);
        }

        @Override
        public void writeTo(DropTableEntry entry, CatalogObjectDataOutput out) throws IOException {
            out.writeVarInt(entry.tableId());
        }
    }

    /**
     * Serializer for {@link DropTableEntry}.
     */
    @CatalogSerializer(version = 1, since = "3.1.0")
    static class DropTableEntrySerializerV2 implements CatalogObjectSerializer<DropTableEntry> {
        @Override
        public DropTableEntry readFrom(CatalogObjectDataInput input)throws IOException {
            int tableId = input.readVarIntAsInt();

            return new DropTableEntry(tableId);
        }

        @Override
        public void writeTo(DropTableEntry entry, CatalogObjectDataOutput out) throws IOException {
            out.writeVarInt(entry.tableId());
        }
    }
}
