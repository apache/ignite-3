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

import static org.apache.ignite.internal.catalog.CatalogManager.INITIAL_TIMESTAMP;

import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntry;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tostring.S;

/**
 * System view descriptor.
 */
public class CatalogSystemViewDescriptor extends CatalogObjectDescriptor implements MarshallableEntry, CatalogColumnContainer {
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
    public CatalogSystemViewDescriptor(
            int id,
            int schemaId,
            String name,
            List<CatalogTableColumnDescriptor> columns,
            SystemViewType systemViewType
    ) {
        this(id, schemaId, name, columns, systemViewType, INITIAL_TIMESTAMP);
    }

    /**
     * Constructor.
     *
     * @param id View id.
     * @param schemaId Schema id.
     * @param name View name.
     * @param columns View columns.
     * @param systemViewType View type.
     * @param timestamp Timestamp of the update of the descriptor.
     */
    public CatalogSystemViewDescriptor(
            int id,
            int schemaId,
            String name,
            List<CatalogTableColumnDescriptor> columns,
            SystemViewType systemViewType,
            HybridTimestamp timestamp
    ) {
        super(id, Type.SYSTEM_VIEW, name, timestamp);

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

    /** {@inheritDoc} */
    @Override
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

    @Override
    public int typeId() {
        return MarshallableEntryType.DESCRIPTOR_SYSTEM_VIEW.id();
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
        static SystemViewType forId(int id) {
            switch (id) {
                case 0:
                    return NODE;
                case 1:
                    return CLUSTER;
                default:
                    throw new IllegalArgumentException("Unknown system view type id: " + id);
            }
        }
    }
}
