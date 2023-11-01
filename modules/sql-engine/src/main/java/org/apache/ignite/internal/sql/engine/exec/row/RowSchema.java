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

package org.apache.ignite.internal.sql.engine.exec.row;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.type.NativeType;

/**
 * Row schema used by execution engine.
 */
public final class RowSchema {

    private final List<TypeSpec> fields;

    private RowSchema(List<TypeSpec> types) {
        this.fields = types;
    }

    /** A list of schema fields. */
    public List<TypeSpec> fields() {
        return fields;
    }


    /** {@inheritDoc}. */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowSchema rowSchema = (RowSchema) o;
        return Objects.equals(fields, rowSchema.fields);
    }

    /** {@inheritDoc}. */
    @Override
    public int hashCode() {
        return Objects.hash(fields);
    }

    /** {@inheritDoc}. */
    @Override
    public String toString() {
        return S.toString(RowSchema.class, this, "fields", this.fields);
    }

    /** Creates a builder that creates instances of row schemas. */
    public static Builder builder() {
        return new Builder();
    }

    /** Row schema builder. */
    public static class Builder {

        private final List<TypeSpec> types = new ArrayList<>();

        private Builder() {

        }

        /** Adds a field of the given type. */
        public Builder addField(TypeSpec typeSpec) {
            types.add(typeSpec);
            return this;
        }

        /** Adds a field of the given non-nullable native type. */
        public Builder addField(NativeType nativeType) {
            return addField(nativeType, false);
        }

        /** Adds a field of the given native type with the specified nullability. */
        public Builder addField(NativeType nativeType, boolean nullable) {
            types.add(new BaseTypeSpec(nativeType, nullable));
            return this;
        }

        /** Creates an instance of row schema. */
        public RowSchema build() {
            return new RowSchema(types);
        }
    }
}
