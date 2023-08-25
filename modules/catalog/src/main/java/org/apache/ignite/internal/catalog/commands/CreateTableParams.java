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

import java.util.List;
import org.jetbrains.annotations.Nullable;

/** CREATE TABLE statement. */
public class CreateTableParams extends AbstractTableCommandParams {
    /** Creates parameters builder. */
    public static Builder builder() {
        return new Builder();
    }

    private CreateTableParams() {
        // No-op.
    }

    /** Primary key columns. */
    private List<String> pkCols;

    /** Colocation columns. */
    private List<String> colocationCols;

    /** Columns, {@code null} if not set. */
    private @Nullable List<ColumnParams> cols;

    /** Distribution zone name, {@code null} means to use the default zone. */
    private @Nullable String zone;

    /** Returns table columns. */
    public List<ColumnParams> columns() {
        return cols;
    }

    /** Returns primary key columns. */
    public List<String> primaryKeyColumns() {
        return pkCols;
    }

    /** Returns colocation column names, {@code null} if not set. */
    public @Nullable List<String> colocationColumns() {
        return colocationCols;
    }

    /** Returns zone name, {@code null} means to use the default distribution zone. */
    public @Nullable String zone() {
        return zone;
    }

    /** Parameters builder. */
    public static class Builder extends AbstractTableBuilder<CreateTableParams, Builder> {
        private Builder() {
            super(new CreateTableParams());
        }

        /**
         * Sets table columns.
         *
         * @param cols Columns.
         * @return {@code this}.
         * @throws NullPointerException If the columns is {@code null} or one of its elements.
         */
        public Builder columns(List<ColumnParams> cols) {
            params.cols = List.copyOf(cols);

            return this;
        }

        /**
         * Sets primary key columns.
         *
         * @return {@code this}.
         * @throws NullPointerException If the primary key columns is {@code null} or one of its elements.
         */
        public Builder primaryKeyColumns(List<String> pkCols) {
            params.pkCols = List.copyOf(pkCols);

            return this;
        }

        /**
         * Sets colocation column names.
         *
         * @param colocationCols Colocation column names.
         * @return {@code this}.
         * @throws NullPointerException If the colocation column names is {@code null} or one of its elements.
         */
        public Builder colocationColumns(List<String> colocationCols) {
            params.colocationCols = colocationCols;

            return this;
        }

        /**
         * Sets zone name.
         *
         * @param zoneName Zone name, {@code null} to use to use the default distribution zone.
         * @return {@code this}.
         */
        public Builder zone(@Nullable String zoneName) {
            params.zone = zoneName;

            return this;
        }
    }
}
