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

import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.apache.ignite.internal.tostring.S;

/**
 * Base class for catalog objects.
 * TODO: IGNITE-19082 Implement custom effective serialization instead.
 */
public abstract class CatalogObjectDescriptor implements Serializable {
    private static final long serialVersionUID = -6525237234280004860L;
    private final int id;
    private final String name;
    private final Type type;
    private long lastUpdateToken;

    CatalogObjectDescriptor(int id, Type type, String name) {
        this.id = id;
        this.type = Objects.requireNonNull(type, "type");
        this.name = Objects.requireNonNull(name, "name");
        this.lastUpdateToken = INITIAL_CAUSALITY_TOKEN;
    }

    CatalogObjectDescriptor(int id, Type type, String name, long causalityToken) {
        this.id = id;
        this.type = Objects.requireNonNull(type, "type");
        this.name = Objects.requireNonNull(name, "name");
        this.lastUpdateToken = causalityToken;
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
     * Token of the last update of the descriptor.
     * Updated every time when {@link UpdateEntry#applyUpdate(org.apache.ignite.internal.catalog.Catalog, long)} is called for the
     * corresponding catalog descriptor.
     *
     * @return Token of the last update of the descriptor
     */
    public long lastUpdateToken() {
        return lastUpdateToken;
    }

    /**
     * Set token of the last update of the descriptor.
     *
     * @param lastUpdateToken Last update token of the descriptor.
     */
    public void lastUpdateToken(long lastUpdateToken) {
        this.lastUpdateToken = lastUpdateToken;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
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
