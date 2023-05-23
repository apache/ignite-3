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

    /** Column default value. */
    private DefaultValue defaultValueDefinition;

    private Integer length = 0;

    private Integer precision = -1;

    private Integer scale = Integer.MIN_VALUE;


    private ColumnParams() {}

    /** Creates a column definition. */
    public ColumnParams(String name, ColumnType type, DefaultValue defaultValueDefinition, boolean nullable) {
        this.name = Objects.requireNonNull(name, "name");
        this.type = Objects.requireNonNull(type, "type");
        this.defaultValueDefinition = Objects.requireNonNull(defaultValueDefinition, "defaultValueDefinition");
        this.nullable = nullable;
    }

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
    @SuppressWarnings("unchecked")
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
     * Get column's precision.
     */
    public Integer precision() {
        return precision;
    }

    /**
     * Get column's scale.
     */
    public Integer scale() {
        return scale;
    }

    public Integer length() {
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
            params.nullable = true;

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

        public Builder precision(int precision) {
            params.precision = precision;

            return this;
        }

        public Builder scale(int scale) {
            params.scale = scale;

            return this;
        }

        public Builder length(int length) {
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