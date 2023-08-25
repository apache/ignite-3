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

import org.jetbrains.annotations.Nullable;

/** Abstract table ddl command. */
public abstract class AbstractTableCommandParams implements DdlCommandParams {
    /** Table name. */
    protected String tableName;

    /** Schema name, {@code null} means to use the default schema. */
    protected @Nullable String schema;

    /** Returns table name. */
    public String tableName() {
        return tableName;
    }

    /** Returns schema name, {@code null} means to use the default schema. */
    public @Nullable String schemaName() {
        return schema;
    }

    /** Parameters builder. */
    protected abstract static class AbstractTableBuilder<ParamT extends AbstractTableCommandParams, BuilderT> {
        protected ParamT params;

        AbstractTableBuilder(ParamT params) {
            this.params = params;
        }

        /**
         * Sets schema name.
         *
         * @param schemaName Schema name, {@code null} to use to use the default schema.
         * @return {@code this}.
         */
        public BuilderT schemaName(@Nullable String schemaName) {
            params.schema = schemaName;

            return (BuilderT) this;
        }

        /**
         * Sets table name.
         *
         * @param tableName Table name.
         * @return {@code this}.
         */
        public BuilderT tableName(String tableName) {
            params.tableName = tableName;

            return (BuilderT) this;
        }

        /** Builds parameters. */
        public ParamT build() {
            ParamT params0 = params;
            params = null;
            return params0;
        }
    }
}
