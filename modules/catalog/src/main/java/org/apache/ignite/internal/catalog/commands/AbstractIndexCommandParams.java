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
public class AbstractIndexCommandParams implements DdlCommandParams {
    /** Index name. */
    protected String indexName;

    /** Schema name where this new index will be created. */
    protected String schema;

    /** Unique index flag. */
    protected boolean unique;

    /** Quietly ignore this command if index existence check failed. */
    protected boolean ifIndexExists;

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
     * Returns {@code true} if index is unique, {@code false} otherwise.
     */
    public boolean isUnique() {
        return unique;
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
         * Set quietly ignore flag.
         *
         * @param ifIndexExists Flag.
         */
        public BuilderT ifIndexExists(boolean ifIndexExists) {
            params.ifIndexExists = ifIndexExists;

            return (BuilderT) this;
        }

        /**
         * Sets unique flag.
         */
        public BuilderT unique() {
            params.unique = true;

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
