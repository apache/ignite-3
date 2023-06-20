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

/**
 * Abstract index ddl command.
 */
public abstract class AbstractIndexCommandParams implements DdlCommandParams {
    /** Index name. */
    protected String indexName;

    /** Schema name where this new index will be created. */
    protected String schema;

    /** Table name. */
    protected String tableName;

    /** Unique index flag. */
    protected boolean uniq;

    /**
     * Returns index simple name.
     */
    public String indexName() {
        return indexName;
    }

    /**
     * Returns schema name.
     */
    public String schemaName() {
        return schema;
    }

    /**
     * Returns table name.
     */
    public String tableName() {
        return tableName;
    }

    /**
     * Returns unique index flag.
     */
    public boolean uniq() {
        return uniq;
    }

    /**
     * Parameters builder.
     */
    protected abstract static class AbstractBuilder<ParamT extends AbstractIndexCommandParams, BuilderT> {
        protected ParamT params;

        AbstractBuilder(ParamT params) {
            this.params = params;
        }

        /**
         * Sets schema name.
         *
         * @param schemaName Schema name.
         * @return {@code this}.
         */
        public BuilderT schemaName(String schemaName) {
            params.schema = schemaName;
            return (BuilderT) this;
        }

        /**
         * Sets index simple name.
         *
         * @param indexName Index simple name.
         * @return {@code this}.
         */
        public BuilderT indexName(String indexName) {
            params.indexName = indexName;
            return (BuilderT) this;
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
         * Sets unique index flag.
         *
         * @param uniq Unique index flag.
         * @return {@code this}.
         */
        public BuilderT uniq(boolean uniq) {
            params.uniq = uniq;

            return (BuilderT) this;
        }

        /**
         * Builds parameters.
         *
         * @return Parameters.
         */
        public ParamT build() {
            ParamT params0 = params;
            params = null;
            return params0;
        }
    }
}
