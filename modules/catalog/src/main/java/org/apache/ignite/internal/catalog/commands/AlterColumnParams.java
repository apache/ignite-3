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

/**
 * ALTER TABLE ... ALTER COLUMN statement.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class AlterColumnParams extends AbstractTableCommandParams {
    private String columnName;

    private ColumnType type;

    private Integer precision;

    private Integer scale;

    private Boolean notNull;

    private Function<ColumnType, DefaultValue> resolveDfltFunc;

    /** Returns column name. */
    public String columnName() {
        return columnName;
    }

    public @Nullable ColumnType type() {
        return type;
    }

    public @Nullable Integer precision() {
        return precision;
    }

    public @Nullable Integer scale() {
        return scale;
    }

    public @Nullable Boolean notNull() {
        return notNull;
    }

    public @Nullable Function<ColumnType, DefaultValue> resolveDfltFunc() {
        return resolveDfltFunc;
    }

    public static AlterColumnParams.Builder builder() {
        return new AlterColumnParams.Builder();
    }

    /**
     * Parameters builder.
     */
    public static class Builder extends AbstractBuilder<AlterColumnParams, Builder> {
        private Builder() {
            super(new AlterColumnParams());
        }

        /** Sets column name. */
        public Builder columnName(String name) {
            params.columnName = name;

            return this;
        }

        /** Sets column type. */
        public Builder type(ColumnType type) {
            params.type = type;

            return this;
        }

        /** Sets precision. */
        public Builder precision(int precision) {
            params.precision = precision;

            return this;
        }

        /** Sets scale. */
        public Builder scale(int scale) {
            params.scale = scale;

            return this;
        }

        /** Sets column precision. */
        public Builder notNull(boolean notNull) {
            params.notNull = notNull;

            return this;
        }

        /** Sets column name. */
        public Builder defaultResolver(Function<ColumnType, DefaultValue> resolveDfltFunc) {
            params.resolveDfltFunc = resolveDfltFunc;

            return this;
        }
    }
}
