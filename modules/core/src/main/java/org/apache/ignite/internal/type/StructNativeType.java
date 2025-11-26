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

package org.apache.ignite.internal.type;

import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.sql.ColumnType;

/**
 * Represents a structured native type that contains multiple named fields.
 *
 * <p>A struct type is a composite data type that groups together zero or more fields, each with its own name, type, and nullability
 * constraint.
 *
 * @see NativeType
 * @see Field
 */
public class StructNativeType extends NativeType {
    private final List<Field> fields;

    StructNativeType(List<Field> fields) {
        super(ColumnType.STRUCT, false, 0);

        this.fields = fields;
    }

    /** Returns the list of fields in this struct. */
    public List<Field> fields() {
        return fields;
    }

    /** Returns the number of fields in this struct. */
    public int fieldsCount() {
        return fields.size();
    }

    /**
     * Represents a single field within a struct type.
     *
     * <p>Each field has a name, a native type, and a nullability flag. Fields are immutable once created and can be compared for equality
     * based on all three properties.
     */
    public static class Field {
        private final String name;
        private final NativeType type;
        private final boolean nullable;

        Field(String name, NativeType type, boolean nullable) {
            this.name = name;
            this.type = type;
            this.nullable = nullable;
        }

        /** Returns the name of this field. */
        public String name() {
            return name;
        }

        /** Returns the type of this field. */
        public NativeType type() {
            return type;
        }

        /** Returns whether this field can contain null values. */
        public boolean nullable() {
            return nullable;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Field field = (Field) o;
            return nullable == field.nullable && Objects.equals(name, field.name) && Objects.equals(type, field.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type, nullable);
        }

        @Override
        public String toString() {
            return S.toString(this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        StructNativeType that = (StructNativeType) o;
        return Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields);
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
