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

package org.apache.ignite.internal.catalog.descriptors;

import static org.apache.ignite.internal.catalog.descriptors.CatalogTableSchemaVersions.assignColumnIds;
import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.readArray;
import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.writeArray;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableSchemaVersions.IdGenerator;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableSchemaVersions.TableVersion;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogEntrySerializerProvider;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataInput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataOutput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;

/**
 * Serializers for {@link CatalogTableSchemaVersions}.
 */
public class CatalogTableSchemaVersionsSerializers {
    /**
     * Serializer for {@link CatalogTableSchemaVersions}.
     */
    @CatalogSerializer(version = 1, since = "3.0.0")
    static class TableSchemaVersionsSerializerV1 implements CatalogObjectSerializer<CatalogTableSchemaVersions> {
        private final CatalogEntrySerializerProvider serializers;

        public TableSchemaVersionsSerializerV1(CatalogEntrySerializerProvider serializers) {
            this.serializers = serializers;
        }

        @Override
        public CatalogTableSchemaVersions readFrom(CatalogObjectDataInput input) throws IOException {
            CatalogObjectSerializer<TableVersion> serializer =
                    serializers.get(1, MarshallableEntryType.DESCRIPTOR_TABLE_VERSION.id());

            TableVersion[] versions = readArray(serializer, input, TableVersion.class);
            int base = input.readVarIntAsInt();

            IdGenerator idGenerator = new IdGenerator(0);
            versions = assignColumnIds(idGenerator, versions);

            return new CatalogTableSchemaVersions(base, idGenerator.nextId(), versions);
        }

        @Override
        public void writeTo(CatalogTableSchemaVersions tabVersions, CatalogObjectDataOutput output) throws IOException {
            CatalogObjectSerializer<TableVersion> serializer =
                    serializers.get(1, MarshallableEntryType.DESCRIPTOR_TABLE_VERSION.id());

            writeArray(tabVersions.versions(), serializer, output);
            output.writeVarInt(tabVersions.earliestVersion());
        }
    }

    /**
     * Serializer for {@link CatalogTableSchemaVersions}.
     */
    @CatalogSerializer(version = 2, since = "3.1.0")
    static class TableSchemaVersionsSerializerV2 implements CatalogObjectSerializer<CatalogTableSchemaVersions> {

        @Override
        public CatalogTableSchemaVersions readFrom(CatalogObjectDataInput input) throws IOException {
            List<TableVersion> versionsList = input.readCompactEntryList(TableVersion.class);
            int base = input.readVarIntAsInt();

            IdGenerator idGenerator = new IdGenerator(0);
            TableVersion[] versions = assignColumnIds(idGenerator, versionsList.toArray(new TableVersion[0]));

            return new CatalogTableSchemaVersions(base, idGenerator.nextId(), versions);
        }

        @Override
        public void writeTo(CatalogTableSchemaVersions tabVersions, CatalogObjectDataOutput output) throws IOException {
            output.writeCompactEntryList(Arrays.asList(tabVersions.versions()));
            output.writeVarInt(tabVersions.earliestVersion());
        }
    }

    @CatalogSerializer(version = 3, since = "3.2.0")
    static class TableSchemaVersionsSerializerV3 implements CatalogObjectSerializer<CatalogTableSchemaVersions> {

        @Override
        public CatalogTableSchemaVersions readFrom(CatalogObjectDataInput input) throws IOException {
            List<TableVersion> versions = input.readCompactEntryList(TableVersion.class);
            int base = input.readVarIntAsInt();
            int nextColumnId = input.readVarIntAsInt();

            return new CatalogTableSchemaVersions(base, nextColumnId, versions.toArray(new TableVersion[0]));
        }

        @Override
        public void writeTo(CatalogTableSchemaVersions tabVersions, CatalogObjectDataOutput output) throws IOException {
            output.writeCompactEntryList(Arrays.asList(tabVersions.versions()));
            output.writeVarInt(tabVersions.earliestVersion());
            output.writeVarInt(tabVersions.nextColumnId());
        }
    }
}
