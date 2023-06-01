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
import org.apache.ignite.sql.ColumnType;

/**
 * Table column type descriptor.
 */
public class TypeDescriptor implements Serializable {
    private static final long serialVersionUID = 1124782342174117133L;

    private final ColumnType type;

    private final int precision;

    private final int scale;

    /** Constructs type descriptor using only mandatory parameters. */
    public TypeDescriptor(ColumnType type) {
        this(type, 0, 0);
    }

    /**
     * Constructor.
     *
     * @param type Column type.
     * @param precision Column precision.
     * @param scale Column scale.
     */
    public TypeDescriptor(ColumnType type, int precision, int scale) {
        this.type = type;
        this.precision = precision;
        this.scale = scale;
    }

    public ColumnType type() {
        return type;
    }

    public int precision() {
        return precision;
    }

    public int scale() {
        return scale;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TypeDescriptor that = (TypeDescriptor) o;

        return precision == that.precision && scale == that.scale && type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, precision, scale);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }
}
