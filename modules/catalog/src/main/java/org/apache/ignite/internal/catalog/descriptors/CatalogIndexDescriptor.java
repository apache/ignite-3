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

/** Index descriptor base class. */
public abstract class CatalogIndexDescriptor extends CatalogObjectDescriptor {
    /** Table ID. */
    private final int tableId;

    /** Unique constraint flag. */
    private final boolean unique;

    /** Index status. */
    private final CatalogIndexStatus status;

    /** Index descriptor type. */
    private final CatalogIndexDescriptorType indexType;

    CatalogIndexDescriptor(
            CatalogIndexDescriptorType indexType,
            int id,
            String name,
            int tableId,
            boolean unique,
            CatalogIndexStatus status,
            long causalityToken
    ) {
        super(id, Type.INDEX, name, causalityToken);
        this.indexType = indexType;
        this.tableId = tableId;
        this.unique = unique;
        this.status = Objects.requireNonNull(status, "status");
    }

    /** Gets table ID. */
    public int tableId() {
        return tableId;
    }

    /** Gets index unique flag. */
    public boolean unique() {
        return unique;
    }

    /** Returns index status. */
    public CatalogIndexStatus status() {
        return status;
    }

    /** Returns catalog index descriptor type. */
    public CatalogIndexDescriptorType indexType() {
        return indexType;
    }

    @Override
    public String toString() {
        return S.toString(CatalogIndexDescriptor.class, this, super.toString());
    }

    /** Catalog index descriptor type. */
    public enum CatalogIndexDescriptorType {
        HASH(0),
        SORTED(1);

        private final int typeId;

        CatalogIndexDescriptorType(int typeId) {
            this.typeId = typeId;
        }

        public int id() {
            return typeId;
        }

        /** Returns catalog index descriptor type by identifier. */
        public static CatalogIndexDescriptorType forId(int id) {
            switch (id) {
                case 0:
                    return HASH;
                case 1:
                    return SORTED;
                default:
                    throw new IllegalArgumentException("Unknown index descriptor type id: " + id);
            }
        }
    }
}
