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

package org.apache.ignite.internal.marshaller;

import java.util.Objects;
import java.util.function.Supplier;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.jetbrains.annotations.Nullable;

/**
 * Marshaller column.
 */
public class MarshallerColumn {
    /** Default "default value supplier". */
    private static final Supplier<Object> NULL_SUPPLIER = () -> null;

    /**
     * Column name.
     */
    private final String name;

    /**
     * Column type.
     */
    private final BinaryMode type;

    /**
     * Scale.
     */
    private final int scale;

    /**
     * Default value supplier.
     */
    @IgniteToStringExclude
    private final Supplier<Object> defValSup;

    /**
     * Constructor.
     *
     * @param name      Column name.
     * @param type      An instance of column data type.
     */
    public MarshallerColumn(String name, BinaryMode type) {
        this(name, type, null, 0);
    }

    /**
     * Constructor.
     *
     * @param name      Column name.
     * @param type      An instance of column data type.
     * @param defValSup Default value supplier.
     */
    public MarshallerColumn(String name, BinaryMode type, @Nullable Supplier<Object> defValSup, int scale) {
        this.name = name;
        this.type = type;
        this.defValSup = defValSup == null ? NULL_SUPPLIER : defValSup;
        this.scale = scale;
    }

    public String name() {
        return name;
    }

    public BinaryMode type() {
        return type;
    }

    public Object defaultValue() {
        return defValSup.get();
    }

    public int scale() {
        return scale;
    }

    @Override
    public boolean equals(Object o) {
        // NOTE: This code ries on the fact that marshaller for a list of columns is used by client code
        // and client code does not provide `defValSup`. Because of that `defValSup`  does not participate in equality/hashcode.
        // It can't do that anyway, since instances of functional interfaces have no identity.
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MarshallerColumn that = (MarshallerColumn) o;
        return scale == that.scale && Objects.equals(name, that.name) && type == that.type;
    }

    @Override
    public int hashCode() {
        // See comment in equals method.
        return Objects.hash(name, type, scale);
    }
}
