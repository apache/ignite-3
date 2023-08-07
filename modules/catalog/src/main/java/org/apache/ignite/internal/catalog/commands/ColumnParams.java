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

import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/** Defines a particular column within table. */
public class ColumnParams {
    /** Creates parameters builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** Column name. */
    private String name;

    /** Column type. */
    private ColumnType type;

    /** Nullability flag. */
    private boolean nullable;

    /** Column length. */
    private Integer length;

    /** Column precision. */
    private Integer precision;

    /** Column scale. */
    private Integer scale;

    /** Column default value. */
    private DefaultValue defaultValueDefinition = DefaultValue.constant(null);

    /**
     * Get column's name.
     */
    public String name() {
        return name;
    }

    /**
     * Get column's type.
     */
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

    /**
     * Get nullable flag: {@code true} if this column accepts nulls.
     */
    public boolean nullable() {
        return nullable;
    }

    /**
     * Get column's precision or {@code null} if not set.
     */
    public @Nullable Integer precision() {
        return precision;
    }

    /**
     * Get column's scale or {@code null} if not set.
     */
    public @Nullable Integer scale() {
        return scale;
    }

    /**
     * Get column's length or {@code null} if not set.
     */
    public @Nullable Integer length() {
        return length;
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
         * @param name Column simple name.
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
         * @return {@code this}.
         */
        public Builder nullable(boolean nullable) {
            params.nullable = nullable;

            return this;
        }

        /**
         * Sets column default value.
         *
         * @return {@code this}.
         */
        public Builder defaultValue(DefaultValue defaultValue) {
            params.defaultValueDefinition = defaultValue;

            return this;
        }

        /**
         * Sets column precision.
         *
         * @return {@code this}.
         */
        public Builder precision(Integer precision) {
            params.precision = precision;

            return this;
        }

        /**
         * Sets column scale.
         *
         * @return {@code this}.
         */
        public Builder scale(Integer scale) {
            params.scale = scale;

            return this;
        }

        /**
         * Sets column length.
         *
         * @return {@code this}.
         */
        public Builder length(Integer length) {
            params.length = length;

            return this;
        }

        /**
         * Builds parameters.
         *
         * @return Parameters.
         */
        public ColumnParams build() {
            ColumnParams params0 = params;
            params = null;
            return params0;
        }
    }
}
