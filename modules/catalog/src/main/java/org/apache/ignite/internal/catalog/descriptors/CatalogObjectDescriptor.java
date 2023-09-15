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

import java.io.Serializable;
import java.util.Objects;
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

    CatalogObjectDescriptor(int id, Type type, String name) {
        this.id = id;
        this.type = Objects.requireNonNull(type, "type");
        this.name = Objects.requireNonNull(name, "name");
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
