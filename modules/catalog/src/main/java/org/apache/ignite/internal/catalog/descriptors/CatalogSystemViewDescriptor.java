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

import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.tostring.S;

/**
 * System view descriptor.
 */
public class CatalogSystemViewDescriptor extends CatalogObjectDescriptor {

    private static final long serialVersionUID = -245956820725588288L;

    private final List<CatalogTableColumnDescriptor> columns;

    private final SystemViewType systemViewType;

    /**
     * Constructor.
     *
     * @param id View id.
     * @param name View name.
     * @param columns View columns.
     */
    public CatalogSystemViewDescriptor(int id, String name, List<CatalogTableColumnDescriptor> columns, SystemViewType systemViewType) {
        super(id, Type.SYSTEM_VIEW, name, INITIAL_CAUSALITY_TOKEN);

        this.columns = Objects.requireNonNull(columns, "columns");
        this.systemViewType = Objects.requireNonNull(systemViewType, "viewType");
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
        return Objects.equals(columns, that.columns) && systemViewType == that.systemViewType;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(columns, systemViewType);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(CatalogSystemViewDescriptor.class, this, "id", id(), "columns", columns, "systemViewType", systemViewType());
    }

    /**
     * Type of a system view.
     */
    public enum SystemViewType {
        /**
         * Node system view.
         */
        LOCAL,
        /**
         * Cluster-wide system view.
         */
        GLOBAL
    }
}
