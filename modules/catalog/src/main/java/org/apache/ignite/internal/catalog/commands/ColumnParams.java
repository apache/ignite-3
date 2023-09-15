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

package org.apache.ignite.internal.catalog.commands;

import java.util.Objects;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/** Defines a particular column within table. */
public class ColumnParams {
    /** Creates parameters builder. */
    public static Builder builder() {
        return new Builder();
    }

    private ColumnParams() {
        // No-op.
    }

    /** Column name. */
    private String name;

    /** Column type. */
    private ColumnType type;

    /** Nullability flag. */
    private boolean nullable;

    /** Column length, {@code null} if not set. */
    private @Nullable Integer length;

    /** Column precision, {@code null} if not set. */
    private @Nullable Integer precision;

    /** Column scale, {@code null} if not set. */
    private @Nullable Integer scale;

    /** Column default value. */
    private DefaultValue defaultValueDefinition = DefaultValue.constant(null);

    /** Returns column name. */
    public String name() {
        return name;
    }

    /** Returns column type. */
    public ColumnType type() {
        return type;
    }

    /**
     * Returns default value definition.
     *
     * @param <T> Desired subtype of the definition.
     * @return Default value definition.
     */
    public <T extends DefaultValue> T defaultValueDefinition() {
        return (T) defaultValueDefinition;
    }

    /** Returns nullable flag: {@code true} if this column accepts nulls. */
    public boolean nullable() {
        return nullable;
    }

    /** Returns column precision or {@code null} if not set. */
    public @Nullable Integer precision() {
        return precision;
    }

    /** Returns column scale or {@code null} if not set. */
    public @Nullable Integer scale() {
        return scale;
    }

    /** Returns column length or {@code null} if not set. */
    public @Nullable Integer length() {
        return length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnParams that = (ColumnParams) o;
        return nullable == that.nullable && Objects.equals(name, that.name) && type == that.type && Objects.equals(length,
                that.length) && Objects.equals(precision, that.precision) && Objects.equals(scale, that.scale)
                && Objects.equals(defaultValueDefinition, that.defaultValueDefinition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, nullable, length, precision, scale, defaultValueDefinition);
    }

    /** Parameters builder. */
    public static class Builder {
        private ColumnParams params;

        private Builder() {
            params = new ColumnParams();
        }

        /**
         * Set column simple name.
         *
         * @param name Column name.
         * @return {@code this}.
         */
        public Builder name(String name) {
            params.name = name;

            return this;
        }

        /**
         * Set column type.
         *
         * @param type Column type.
         * @return {@code this}.
         */
        public Builder type(ColumnType type) {
            params.type = type;

            return this;
        }

        /**
         * Marks column as nullable.
         *
         * @param nullable {@code true} if this column accepts nulls.
         * @return {@code this}.
         */
        public Builder nullable(boolean nullable) {
            params.nullable = nullable;

            return this;
        }

        /**
         * Sets column default value.
         *
         * @param defaultValue Column default value.
         * @return {@code this}.
         */
        public Builder defaultValue(DefaultValue defaultValue) {
            params.defaultValueDefinition = defaultValue;

            return this;
        }

        /**
         * Sets column precision.
         *
         * @param precision Column precision.
         * @return {@code this}.
         */
        public Builder precision(Integer precision) {
            params.precision = precision;

            return this;
        }

        /**
         * Sets column scale.
         *
         * @param scale Column scale.
         * @return {@code this}.
         */
        public Builder scale(Integer scale) {
            params.scale = scale;

            return this;
        }

        /**
         * Sets column length.
         *
         * @param length Column length.
         * @return {@code this}.
         */
        public Builder length(Integer length) {
            params.length = length;

            return this;
        }

        /** Builds parameters. */
        public ColumnParams build() {
            ColumnParams params0 = params;
            params = null;
            return params0;
        }
    }
}
