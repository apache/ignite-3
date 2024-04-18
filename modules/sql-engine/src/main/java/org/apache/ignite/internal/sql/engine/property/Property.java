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

package org.apache.ignite.internal.sql.engine.property;

import java.util.Objects;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;

/**
 * Describes an arbitrary property.
 *
 * <p>The properties is designed to be used in the following way:
 * <pre>
 *     class MyProperties {
 *         public static final Property&lt;String&gt; STRING_PROPERTY = new Property<>("string_prop_name", String.class);
 *         public static final Property&lt;Long&gt; LONG_PROPERTY = new Property<>("long_prop_name", Long.class);
 *     }
 *
 *     var properties = SqlPropertiesHelper.newBuilder()
 *             .set(MyProperties.STRING_PROPERTY, "the value of a string prop")
 *             .set(MyProperties.LONG_PROPERTY, 42L)
 *             .build();
 *
 *     String stringVal = properties.get(MyProperties.STRING_PROPERTY);
 *     long longVal = properties.getOrDefault(MyProperties.LONG_PROPERTY, 10L);
 * </pre>
 *
 * @param <T> Expected value type with which the property is associated.
 */
public class Property<T> {
    /** Name of the property. */
    @IgniteToStringInclude
    public final String name;

    /** Expected value class with which the property is associated. */
    @IgniteToStringInclude
    public final Class<?> cls;

    /**
     * Constructor.
     *
     * @param name Name of the property.
     * @param cls Value class.
     */
    public Property(String name, Class<T> cls) {
        this.name = Objects.requireNonNull(name, "name");
        this.cls = Objects.requireNonNull(cls, "cls");
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        // reference comparison is intentional
        return this == o;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return name.hashCode();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(Property.class, this);
    }
}
