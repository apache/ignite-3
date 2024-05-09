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

import static org.apache.ignite.internal.catalog.CatalogManagerImpl.INITIAL_CAUSALITY_TOKEN;
import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.readList;
import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.writeList;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * System view descriptor.
 */
public class CatalogSystemViewDescriptor extends CatalogObjectDescriptor {
    public static final CatalogObjectSerializer<CatalogSystemViewDescriptor> SERIALIZER = new SystemViewDescriptorSerializer();

    private final int schemaId;

    private final List<CatalogTableColumnDescriptor> columns;

    private final SystemViewType systemViewType;

    /**
     * Constructor.
     *
     * @param id View id.
     * @param schemaId Schema id.
     * @param name View name.
     * @param columns View columns.
     * @param systemViewType View type.
     */
    public CatalogSystemViewDescriptor(int id, int schemaId, String name, List<CatalogTableColumnDescriptor> columns, SystemViewType systemViewType) {
        this(id, schemaId, name, columns, systemViewType, INITIAL_CAUSALITY_TOKEN);
    }

    /**
     * Constructor.
     *
     * @param id View id.
     * @param schemaId Schema id.
     * @param name View name.
     * @param columns View columns.
     * @param systemViewType View type.
     * @param causalityToken Token of the update of the descriptor.
     */
    public CatalogSystemViewDescriptor(
            int id,
            int schemaId,
            String name,
            List<CatalogTableColumnDescriptor> columns,
            SystemViewType systemViewType,
            long causalityToken
    ) {
        super(id, Type.SYSTEM_VIEW, name, causalityToken);

        this.schemaId = schemaId;
        this.columns = Objects.requireNonNull(columns, "columns");
        this.systemViewType = Objects.requireNonNull(systemViewType, "viewType");
    }

    /**
     * Returns a schema id of this view.
     *
     * @return A schema id.
     */
    public int schemaId() {
        return schemaId;
    }

    /**
     * Returns a list of columns of this view.
     *
     * @return A list of columns.
     */
    public List<CatalogTableColumnDescriptor> columns() {
        return columns;
    }

    /**
     * Returns a type of this view.
     *
     * @return A type of this view.
     */
    public SystemViewType systemViewType() {
        return systemViewType;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CatalogSystemViewDescriptor that = (CatalogSystemViewDescriptor) o;
        return schemaId == that.schemaId && Objects.equals(columns, that.columns) && systemViewType == that.systemViewType;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(schemaId, columns, systemViewType);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(
                CatalogSystemViewDescriptor.class, this,
                "id", id(),
                "schemaId", schemaId,
                "name", name(),
                "columns", columns,
                "systemViewType", systemViewType()
        );
    }

    /**
     * Type of a system view.
     */
    public enum SystemViewType {
        /**
         * Node system view.
         */
        NODE(0),
        /**
         * Cluster-wide system view.
         */
        CLUSTER(1);

        private final int id;

        SystemViewType(int id) {
            this.id = id;
        }

        public int id() {
            return id;
        }

        /** Returns system view type by identifier. */
        private static SystemViewType forId(int id) {
            if (id == 0) {
                return NODE;
            } else {
                assert id == 1;

                return CLUSTER;
            }
        }
    }

    /**
     * Serializer for {@link CatalogSystemViewDescriptor}.
     */
    private static class SystemViewDescriptorSerializer implements CatalogObjectSerializer<CatalogSystemViewDescriptor> {
        @Override
        public CatalogSystemViewDescriptor readFrom(IgniteDataInput input) throws IOException {
            int id = input.readInt();
            int schemaId = input.readInt();
            String name = input.readUTF();
            long updateToken = input.readLong();
            List<CatalogTableColumnDescriptor> columns = readList(CatalogTableColumnDescriptor.SERIALIZER, input);

            byte sysViewTypeId = input.readByte();
            SystemViewType sysViewType = SystemViewType.forId(sysViewTypeId);

            return new CatalogSystemViewDescriptor(id, schemaId, name, columns, sysViewType, updateToken);
        }

        @Override
        public void writeTo(CatalogSystemViewDescriptor descriptor, IgniteDataOutput output) throws IOException {
            output.writeInt(descriptor.id());
            output.writeInt(descriptor.schemaId);
            output.writeUTF(descriptor.name());
            output.writeLong(descriptor.updateToken());
            writeList(descriptor.columns(), CatalogTableColumnDescriptor.SERIALIZER, output);
            output.writeByte(descriptor.systemViewType().id());
        }
    }
}
