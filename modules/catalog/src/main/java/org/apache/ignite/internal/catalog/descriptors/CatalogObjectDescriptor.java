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

import java.util.Objects;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tostring.S;

/**
 * Base class for catalog objects.
 */
public abstract class CatalogObjectDescriptor {
    private final int id;
    private final String name;
    private final Type type;
    private HybridTimestamp updateTimestamp;

    CatalogObjectDescriptor(int id, Type type, String name, HybridTimestamp timestamp) {
        this.id = id;
        this.type = Objects.requireNonNull(type, "type");
        this.name = Objects.requireNonNull(name, "name");
        this.updateTimestamp = timestamp;
    }

    /** Returns id of the described object. */
    public int id() {
        return id;
    }

    /** Returns name of the described object. */
    public String name() {
        return name;
    }

    /** Return schema-object type. */
    public Type type() {
        return type;
    }

    /**
     * Timestamp of the update of the descriptor.
     * Updated when {@link UpdateEntry#applyUpdate(org.apache.ignite.internal.catalog.Catalog, HybridTimestamp)} is called for the
     * corresponding catalog descriptor. This timestamp is the timestamp that is associated with the corresponding update being applied to
     * the Catalog. Any new catalog descriptor associated with an {@link UpdateEntry}, meaning that this token is set only once.
     *
     * @return timestamp of the update of the descriptor.
     */
    public HybridTimestamp updateTimestamp() {
        return updateTimestamp;
    }

    /**
     * Set timestamp of the update of the descriptor. Must be called only once when
     * {@link UpdateEntry#applyUpdate(org.apache.ignite.internal.catalog.Catalog, HybridTimestamp)} is called for the corresponding catalog
     * descriptor. This timestamp is the timestamp that is associated with the corresponding update being applied to
     * the Catalog. Any new catalog descriptor associated with an {@link UpdateEntry}, meaning that this token is set only once.
     *
     * @param updateTimestamp Update timestamp of the descriptor.
     */
    public void updateTimestamp(HybridTimestamp updateTimestamp) {
        assert this.updateTimestamp.equals(INITIAL_TIMESTAMP) : "Update timestamp for the descriptor must be updated only once";

        this.updateTimestamp = updateTimestamp;
    }

    @Override
    public String toString() {
        return S.toString(CatalogObjectDescriptor.class, this);
    }

    /** Catalog object type. */
    public enum Type {
        SCHEMA,
        TABLE,
        INDEX,
        ZONE,
        SYSTEM_VIEW
    }
}
