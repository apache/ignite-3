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

import java.util.function.Function;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/** ALTER TABLE ... ALTER COLUMN statement. */
public class AlterColumnParams extends AbstractTableCommandParams {
    /** Creates parameters builder. */
    public static AlterColumnParams.Builder builder() {
        return new AlterColumnParams.Builder();
    }

    private AlterColumnParams() {
        // No-op.
    }

    private String columnName;

    private @Nullable ColumnType type;

    private @Nullable Integer precision;

    private @Nullable Integer length;

    private @Nullable Integer scale;

    private @Nullable Boolean notNull;

    private @Nullable Function<ColumnType, DefaultValue> defaultResolver;

    /** Returns column name. */
    public String columnName() {
        return columnName;
    }

    /** Returns column type, {@code null} if not set. */
    public @Nullable ColumnType type() {
        return type;
    }

    /** Returns column precision, {@code null} if not set. */
    public @Nullable Integer precision() {
        return precision;
    }

    /** Returns column length, {@code null} if not set. */
    public @Nullable Integer length() {
        return length;
    }

    /** Returns column scale, {@code null} if not set. */
    public @Nullable Integer scale() {
        return scale;
    }

    /** Returns the {@code NOT NULL} constraint change flag, {@code null} if not set. */
    public @Nullable Boolean notNull() {
        return notNull;
    }

    /** Returns a default value depending on the column type, {@code null} if not set. */
    public @Nullable DefaultValue defaultValue(ColumnType type) {
        return defaultResolver == null ? null : defaultResolver.apply(type);
    }

    /** Parameters builder. */
    public static class Builder extends AbstractTableBuilder<AlterColumnParams, Builder> {
        private Builder() {
            super(new AlterColumnParams());
        }

        /**
         * Sets column name.
         *
         * @param name Colum name.
         * @return {@code this}.
         */
        public Builder columnName(String name) {
            params.columnName = name;

            return this;
        }

        /**
         * Sets column type.
         *
         * @param type Column type.
         * @return {@code this}.
         */
        public Builder type(ColumnType type) {
            params.type = type;

            return this;
        }

        /**
         * Sets column precision.
         *
         * @param precision Column precision.
         * @return {@code this}.
         */
        public Builder precision(int precision) {
            params.precision = precision;

            return this;
        }

        /**
         * Sets column length.
         *
         * @param length Column length.
         * @return {@code this}.
         */
        public Builder length(int length) {
            params.length = length;

            return this;
        }

        /**
         * Sets column scale.
         *
         * @param scale Column scale.
         * @return {@code this}.
         */
        public Builder scale(int scale) {
            params.scale = scale;

            return this;
        }

        /**
         * Sets the {@code NOT NULL} constraint change flag.
         *
         * @param notNull {@code NOT NULL} constraint change flag.
         * @return {@code this}.
         */
        public Builder notNull(Boolean notNull) {
            params.notNull = notNull;

            return this;
        }

        /**
         * Sets function that resolves a default value depending on the type of the column.
         *
         * @param resolveDfltFunc Function that resolves a default value depending on the type of the column.
         * @return {@code this}.
         */
        public Builder defaultValueResolver(Function<ColumnType, DefaultValue> resolveDfltFunc) {
            params.defaultResolver = resolveDfltFunc;

            return this;
        }
    }
}
