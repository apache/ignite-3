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

import org.apache.ignite.internal.tostring.S;

/** Index descriptor base class. */
public abstract class CatalogIndexDescriptor extends CatalogObjectDescriptor {
    private static final long serialVersionUID = -8045949593661301287L;

    /** Table id. */
    private final int tableId;

    /** Unique constraint flag. */
    private final boolean unique;

    /**
     * Index availability flag, {@code true} means it is available (the index has been built), otherwise it is registered
     * (the index has not yet been built).
     */
    private final boolean available;

    CatalogIndexDescriptor(int id, String name, int tableId, boolean unique, boolean available) {
        super(id, Type.INDEX, name, INITIAL_CAUSALITY_TOKEN);
        this.tableId = tableId;
        this.unique = unique;
        this.available = available;
    }

    /** Gets table id. */
    public int tableId() {
        return tableId;
    }

    /** Gets index unique flag. */
    public boolean unique() {
        return unique;
    }

    /**
     * Returns index availability flag, {@code true} means it is available (the index has been built), otherwise it is
     * registered (the index has not yet been built).
     */
    public boolean available() {
        return available;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
