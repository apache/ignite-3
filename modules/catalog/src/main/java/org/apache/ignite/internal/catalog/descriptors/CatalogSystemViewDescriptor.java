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

import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.tostring.S;

/**
 * System view descriptor.
 */
public class CatalogSystemViewDescriptor extends CatalogObjectDescriptor {

    private static final long serialVersionUID = -245956820725588288L;

    private final List<CatalogTableColumnDescriptor> columns;

    /**
     * Constructor.
     *
     * @param id View id.
     * @param name View name.
     * @param columns View columns.
     */
    public CatalogSystemViewDescriptor(int id, String name, List<CatalogTableColumnDescriptor> columns) {
        super(id, Type.SYSTEM_VIEW, name);

        this.columns = Objects.requireNonNull(columns, "columns");
    }

    /**
     * Returns a list of columns of this view.
     *
     * @return A list of columns.
     */
    public List<CatalogTableColumnDescriptor> columns() {
        return columns;
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
        return Objects.equals(columns, that.columns);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(columns);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(CatalogSystemViewDescriptor.class, this, "id", id(), "columns", columns, "type", type());
    }
}
