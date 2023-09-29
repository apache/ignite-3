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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.Objects;
import org.apache.ignite.internal.catalog.CatalogParamsValidationUtils;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/** Defines a particular column within table. */
public class ColumnParams {
    public static final String ERR_COL_PARAM_NOT_APPLICABLE = "{} is not applicable for column '{}' of type '{}'";
    public static final String ERR_COL_PARAM_VALIDATION = "{} for column '{}' of type '{}' must be non-negative";
    public static final String ERR_COL_PARAM_DEFINITION = "{} definition is necessary for column '{}' of type '{}'";

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
        public Builder precision(@Nullable Integer precision) {
            params.precision = precision;

            return this;
        }

        /**
         * Sets column scale.
         *
         * @param scale Column scale.
         * @return {@code this}.
         */
        public Builder scale(@Nullable Integer scale) {
            params.scale = scale;

            return this;
        }

        /**
         * Sets column length.
         *
         * @param length Column length.
         * @return {@code this}.
         */
        public Builder length(@Nullable Integer length) {
            params.length = length;

            return this;
        }

        /** Builds parameters. */
        public ColumnParams build() {
            validate();

            ColumnParams params0 = params;
            params = null;
            return params0;
        }

        private void validate() {
            CatalogParamsValidationUtils.validateIdentifier(params.name(), "Column name");

            if (params.type == null) {
                throw new CatalogValidationException(format("Type is not specified for column '{}'", params.name()));
            }

            if (params.type == ColumnType.NULL) {
                throw new CatalogValidationException(format("Type NULL is not applicable for column '{}'", params.name()));
            }

            boolean validatePrecision = params.type.precisionAllowed();
            boolean validateScale = params.type.scaleAllowed();
            boolean validateLenght = params.type.lengthAllowed();

            if (validateLenght) {
                if (params.length() == null) {
                    throw new CatalogValidationException(format(ERR_COL_PARAM_DEFINITION, "Length", params.name(), params.type()));
                }

                if (params.length() < 0) {
                    throw new CatalogValidationException(format(ERR_COL_PARAM_VALIDATION, "Length", params.name(), params.type()));
                }
            } else {
                if (params.length() != null) {
                    throw new CatalogValidationException(format(ERR_COL_PARAM_NOT_APPLICABLE, "Length", params.name(), params.type()));
                }
            }

            if (validatePrecision) {
                validatePrecision(params);

                if (params.scale() != null && !validateScale) {
                    throw new CatalogValidationException(format(ERR_COL_PARAM_NOT_APPLICABLE, "Scale", params.name(), params.type()));
                }
            }

            if (validateScale) {
                validateScale(params);
            }

            if (!validatePrecision && !validateScale) {
                if (params.precision() != null) {
                    throw new CatalogValidationException(format(ERR_COL_PARAM_NOT_APPLICABLE, "Precision", params.name(), params.type()));
                }

                if (params.scale() != null) {
                    throw new CatalogValidationException(format(ERR_COL_PARAM_NOT_APPLICABLE, "Scale", params.name(), params.type()));
                }
            }
        }

        private static void validatePrecision(ColumnParams params) {
            Integer precision = params.precision();

            if (precision == null) {
                throw new CatalogValidationException(format(ERR_COL_PARAM_DEFINITION, "Precision", params.name(), params.type()));
            }

            if (precision < 0) {
                throw new CatalogValidationException(format(ERR_COL_PARAM_VALIDATION, "Precision", params.name(), params.type()));
            }
        }

        private static void validateScale(ColumnParams params) {
            Integer scale = params.scale();

            if (scale == null) {
                throw new CatalogValidationException(format(ERR_COL_PARAM_DEFINITION, "Scale", params.name(), params.type()));
            }

            if (scale < 0) {
                throw new CatalogValidationException(format(ERR_COL_PARAM_VALIDATION, "Scale", params.name(), params.type()));
            }
        }
    }
}
