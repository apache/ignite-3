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

import java.util.Objects;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Indexed column descriptor.
 */
public class CatalogIndexColumnDescriptor {
    private final int columnId;
    private final @Nullable String name;

    private final CatalogColumnCollation collation;

    /**
     * Constructs the object.
     *
     * @param name Name of the indexed column.
     * @param collation Collation of the indexed column.
     * @deprecated This constructor is used in old deserializers. Use {@link #CatalogIndexColumnDescriptor(int, CatalogColumnCollation)}
     *         instead.
     */
    @Deprecated(forRemoval = true)
    public CatalogIndexColumnDescriptor(String name, CatalogColumnCollation collation) {
        this.columnId = CatalogTableColumnDescriptor.ID_IS_NOT_ASSIGNED;
        this.name = Objects.requireNonNull(name, "name");
        this.collation = Objects.requireNonNull(collation, "collation");
    }

    /**
     * Constructs the object.
     *
     * @param columnId ID of the indexed column.
     * @param collation Collation of the indexed column.
     */
    public CatalogIndexColumnDescriptor(int columnId, CatalogColumnCollation collation) {
        assert columnId != CatalogTableColumnDescriptor.ID_IS_NOT_ASSIGNED;

        this.columnId = columnId;
        this.name = null;
        this.collation = Objects.requireNonNull(collation, "collation");
    }

    /** Returns ID of the indexed column. */
    public int columnId() {
        if (columnId == CatalogTableColumnDescriptor.ID_IS_NOT_ASSIGNED) {
            throw new IllegalStateException("Cannot return columnId from non-upgraded index descriptor");
        }

        return columnId;
    }

    /**
     * Returns name of the indexed column.
     *
     * @return Name of the indexed column.
     * @deprecated Non-null may be returned only during catalog recovery after cluster upgrade.
     *      Use {@link #columnId()} instead.
     */
    @Deprecated(forRemoval = true) // still used in old serializers and in upgrade logic
    public @Nullable String name() {
        return name;
    }

    public CatalogColumnCollation collation() {
        return collation;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }
}
