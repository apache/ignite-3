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

/** ALTER TABLE ... ADD COLUMN statement. */
public class AlterTableAddColumnParams extends AbstractTableCommandParams {
    /** Creates parameters builder. */
    public static Builder builder() {
        return new Builder();
    }

    private AlterTableAddColumnParams() {
        // No-op.
    }

    /** Columns. */
    private List<ColumnParams> cols;

    /** Returns columns that should be added to a table. */
    public List<ColumnParams> columns() {
        return cols;
    }

    /** Parameters builder. */
    public static class Builder extends AbstractTableBuilder<AlterTableAddColumnParams, Builder> {
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
            params.cols = List.copyOf(cols);

            return this;
        }
    }
}
