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

package org.apache.ignite.internal.sql.engine.type;

import java.util.Objects;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFamily;

/**
 * Provides a basic implementation of {@link RelDataTypeFamily} that can be used by {@link IgniteCustomType custom data types}.
 *
 * @see RelDataType#getFamily()
 */
public class IgniteCustomTypeFamily implements RelDataTypeFamily {

    /** The name of this type family. */
    private final String name;

    /**
     * Creates an instance of a type family with the given name.
     *
     * <p>The {@code name} parameter must be the same as {@link IgniteCustomType#getCustomTypeName() custom type name} of a type it
     * is created for.
     *
     * @param name The name.
     */
    public IgniteCustomTypeFamily(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IgniteCustomTypeFamily that = (IgniteCustomTypeFamily) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return name;
    }
}
