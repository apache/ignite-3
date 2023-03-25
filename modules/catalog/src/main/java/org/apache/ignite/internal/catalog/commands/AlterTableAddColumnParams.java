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

/**
 * ALTER TABLE ... ADD COLUMN statement.
 */
public class AlterTableAddColumnParams extends AbstractTableCommandParams {
    private static final long serialVersionUID = 4208842258200745736L;

    public static Builder builder() {
        return new Builder();
    }

    /** Quietly ignore this command if column already exists. */
    private boolean ifColumnNotExists;

    /** Columns. */
    private List<ColumnParams> cols;

    public List<ColumnParams> columns() {
        return cols;
    }

    /**
     * Not exists flag.
     *
     * @return Quietly ignore this command if column exists.
     */
    public boolean ifColumnNotExists() {
        return ifColumnNotExists;
    }

    /**
     * Parameters builder.
     */
    public static class Builder extends AbstractTableCommandParams.AbstractBuilder<AlterTableAddColumnParams, Builder> {
        private Builder() {
            super(new AlterTableAddColumnParams());
        }

        /**
         * Columns to add.
         *
         * @param cols Columns.
         * @return {@code this}.
         */
        public Builder columns(List<ColumnParams> cols) {
            params.cols = cols;
            return this;
        }

        /**
         * Set exists flag.
         *
         * @param ifColumnNotExists Quietly ignore this command if column exists.
         * @return {@code this}.
         */
        public Builder ifColumnNotExists(boolean ifColumnNotExists) {
            params.ifColumnNotExists = ifColumnNotExists;
            return this;
        }
    }
}
