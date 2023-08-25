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

/** Abstract create index ddl command. */
public abstract class AbstractIndexCommandParams implements DdlCommandParams {
    /** Index name. */
    protected String indexName;

    /** Schema name, {@code null} means to use the default schema. */
    protected @Nullable String schemaName;

    /** Returns index name. */
    public String indexName() {
        return indexName;
    }

    /** Returns schema name, {@code null} means to use the default schema. */
    public @Nullable String schemaName() {
        return schemaName;
    }

    /** Parameters builder. */
    protected abstract static class AbstractIndexBuilder<ParamT extends AbstractCreateIndexCommandParams, BuilderT> {
        protected ParamT params;

        /** Constructor. */
        AbstractIndexBuilder(ParamT params) {
            this.params = params;
        }

        /**
         * Sets schema name.
         *
         * @param schemaName Schema name, {@code null} to use to use the default schema.
         * @return {@code this}.
         */
        public BuilderT schemaName(@Nullable String schemaName) {
            params.schemaName = schemaName;

            return (BuilderT) this;
        }

        /**
         * Sets index name.
         *
         * @param indexName Index name.
         * @return {@code this}.
         */
        public BuilderT indexName(String indexName) {
            params.indexName = indexName;

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
