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
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/** Primary key that uses hash index. */
public class TableHashPrimaryKey extends TablePrimaryKey {
    /**
     * Constructor.
     *
     * @param columns List of columns.
     */
    private TableHashPrimaryKey(@Nullable String name, List<String> columns) {
        super(name, columns);
    }

    /** Returns builder to create a primary key that uses a hash index. */
    public static Builder builder() {
        return new Builder();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(TableHashPrimaryKey.class, this, "columns", columns());
    }

    /** Builder to create a primary index that uses a hash index. */
    public static class Builder extends TablePrimaryKeyBuilder<Builder> {
        private @Nullable String name;
        private List<String> columns;

        Builder() {

        }

        /** {@inheritDoc} */
        @Override
        public Builder columns(List<String> columns) {
            this.columns = columns;
            return this;
        }

        /** {@inheritDoc} */
        @Override
        public Builder name(@Nullable String name) {
            this.name = name;
            return this;
        }

        /** Crates a primary key that uses a hash index. */
        @Override
        public TableHashPrimaryKey build() {
            return new TableHashPrimaryKey(name, columns);
        }
    }
}
