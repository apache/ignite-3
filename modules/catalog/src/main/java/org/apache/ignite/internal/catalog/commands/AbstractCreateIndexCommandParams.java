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

/** Abstract create index ddl command. */
public abstract class AbstractCreateIndexCommandParams extends AbstractIndexCommandParams {
    /** Table name. */
    protected String tableName;

    /** Unique index flag. */
    protected boolean unique;

    /** Indexed columns. */
    protected List<String> columns;

    /** Returns table name. */
    public String tableName() {
        return tableName;
    }

    /** Returns {@code true} if index is unique, {@code false} otherwise. */
    public boolean unique() {
        return unique;
    }

    /** Returns index columns. */
    public List<String> columns() {
        return columns;
    }

    /** Parameters builder. */
    protected abstract static class AbstractCreateIndexBuilder<ParamT extends AbstractCreateIndexCommandParams, BuilderT> extends
            AbstractIndexBuilder<ParamT, BuilderT> {
        AbstractCreateIndexBuilder(ParamT params) {
            super(params);
        }

        /**
         * Set table name.
         *
         * @param tableName Table name.
         * @return {@code this}.
         */
        public BuilderT tableName(String tableName) {
            params.tableName = tableName;

            return (BuilderT) this;
        }

        /**
         * Sets unique flag.
         *
         * @param uniq {@code true} if index is unique, {@code false} otherwise.
         * @return {@code this}.
         */
        public BuilderT unique(boolean uniq) {
            params.unique = uniq;

            return (BuilderT) this;
        }

        /**
         * Set columns names.
         *
         * @param columns Columns names.
         * @return {@code this}.
         * @throws NullPointerException If the columns is {@code null} or one of its elements.
         */
        public BuilderT columns(List<String> columns) {
            params.columns = List.copyOf(columns);

            return (BuilderT) this;
        }
    }
}
