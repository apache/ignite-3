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

import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.readArray;
import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.readList;
import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.writeArray;
import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.writeList;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.jetbrains.annotations.Nullable;

/**
 * Class that holds a list of table version descriptors.
 */
public class CatalogTableSchemaVersions {
    public static final CatalogObjectSerializer<CatalogTableSchemaVersions> SERIALIZER = new TableSchemaVersionsSerializer();

    /**
     * Descriptor of a single table version.
     */
    public static class TableVersion {
        private final List<CatalogTableColumnDescriptor> columns;

        public TableVersion(List<CatalogTableColumnDescriptor> columns) {
            this.columns = columns;
        }

        public List<CatalogTableColumnDescriptor> columns() {
            return Collections.unmodifiableList(columns);
        }
    }

    private final int base;
    private final TableVersion[] versions;

    /**
     * Constructor.
     *
     * @param versions Array of table versions.
     */
    CatalogTableSchemaVersions(TableVersion... versions) {
        this(CatalogTableDescriptor.INITIAL_TABLE_VERSION, versions);
    }

    private CatalogTableSchemaVersions(int base, TableVersion... versions) {
        this.base = base;
        this.versions = versions;
    }

    /**
     * Returns earliest known table version.
     */
    public int earliestVersion() {
        return base;
    }

    /**
     * Returns latest known table version.
     */
    public int latestVersion() {
        return base + versions.length - 1;
    }

    /**
     * Returns an existing table version, or {@code null} if it's not found.
     */
    public @Nullable TableVersion get(int version) {
        if (version < base || version >= base + versions.length) {
            return null;
        }

        return versions[version - base];
    }

    /**
     * Creates a new instance of {@link CatalogTableSchemaVersions} with one new version appended.
     */
    public CatalogTableSchemaVersions append(TableVersion tableVersion, int version) {
        assert version == latestVersion() + 1;

        return new CatalogTableSchemaVersions(base, ArrayUtils.concat(versions, tableVersion));
    }

    /**
     * Serializer for {@link CatalogTableSchemaVersions}.
     */
    private static class TableSchemaVersionsSerializer implements CatalogObjectSerializer<CatalogTableSchemaVersions> {
        @Override
        public CatalogTableSchemaVersions readFrom(IgniteDataInput input) throws IOException {
            TableVersion[] versions = readArray(TableVersionSerializer.INSTANCE, input, TableVersion.class);
            int base = input.readInt();

            return new CatalogTableSchemaVersions(base, versions);
        }

        @Override
        public void writeTo(CatalogTableSchemaVersions tabVersions, IgniteDataOutput output) throws IOException {
            writeArray(tabVersions.versions, TableVersionSerializer.INSTANCE, output);
            output.writeInt(tabVersions.base);
        }
    }

    /**
     * Serializer for {@link TableVersion}.
     */
    private static class TableVersionSerializer implements CatalogObjectSerializer<TableVersion> {
        static CatalogObjectSerializer<TableVersion> INSTANCE = new TableVersionSerializer();

        @Override
        public TableVersion readFrom(IgniteDataInput input) throws IOException {
            List<CatalogTableColumnDescriptor> columns = readList(CatalogTableColumnDescriptor.SERIALIZER, input);

            return new TableVersion(columns);
        }

        @Override
        public void writeTo(TableVersion tableVersion, IgniteDataOutput output) throws IOException {
            writeList(tableVersion.columns(), CatalogTableColumnDescriptor.SERIALIZER, output);
        }
    }
}
