/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.schema;

import java.util.Objects;
import org.apache.ignite.schema.SchemaObject;

/**
 * Schema object base class.
 */
public abstract class AbstractSchemaObject implements SchemaObject {
    /** Schema object name. */
    private final String name;

    /**
     * Constructor.
     *
     * @param name Schema object name.
     */
    protected AbstractSchemaObject(String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AbstractSchemaObject object = (AbstractSchemaObject)o;
        return Objects.equals(name, object.name);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(name);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "SchemaObject[" +
            "name='" + name + '\'' +
            "class=" + this.getClass().getName() +
            ']';
    }
}
