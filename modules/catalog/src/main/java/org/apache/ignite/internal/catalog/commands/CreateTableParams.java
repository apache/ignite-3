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

/**
 * CREATE TABLE statement.
 */
public class CreateTableParams extends AbstractTableCommandParams {
    /** Creates parameters builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** Primary key columns. */
    @Nullable
    private List<String> pkCols;

    /** Colocation columns. */
    @Nullable
    private List<String> colocationCols;

    /** Columns. */
    private List<ColumnParams> cols;

    /** Distribution zone name. */
    @Nullable
    private String zone;

    private CreateTableParams() {
    }

    /**
     * Gets table columns.
     */
    public List<ColumnParams> columns() {
        return cols;
    }

    /**
     * Gets primary key columns.
     */
    public List<String> primaryKeyColumns() {
        return pkCols;
    }

    /**
     * Gets colocation column names.
     */
    @Nullable
    public List<String> colocationColumns() {
        return colocationCols;
    }

    /**
     * Gets zone name.
     */
    @Nullable
    public String zone() {
        return zone;
    }

    /**
     * Parameters builder.
     */
    public static class Builder extends AbstractBuilder<CreateTableParams, Builder> {
        private Builder() {
            super(new CreateTableParams());
        }

        /**
         * Sets table columns.
         *
         * @param cols Columns.
         * @return {@code this}.
         */
        public Builder columns(List<ColumnParams> cols) {
            params.cols = cols;

            return this;
        }

        /**
         * Sets primary key columns.
         *
         * @return {@code this}.
         */
        public Builder primaryKeyColumns(List<String> pkCols) {
            params.pkCols = pkCols;

            return this;
        }

        /**
         * Sets colocation column names.
         *
         * @param colocationCols Colocation column names.
         * @return {@code this}.
         */
        public Builder colocationColumns(@Nullable List<String> colocationCols) {
            params.colocationCols = colocationCols;

            return this;
        }

        /**
         * Sets zone name.
         *
         * @param zoneName Zone name.
         * @return {@code this}.
         */
        public Builder zone(@Nullable String zoneName) {
            params.zone = zoneName;

            return this;
        }
    }
}
