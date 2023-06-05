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
 * DROP INDEX statement.
 */
public class DropIndexParams implements DdlCommandParams {
    /** Creates parameters builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** Index name. */
    protected String indexName;

    /** Schema name where this new index will be created. */
    protected String schema;

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
     * Parameters builder.
     */
    public static class Builder {
        private DropIndexParams params;

        Builder() {
            this.params = new DropIndexParams();
        }

        /**
         * Sets schema name.
         *
         * @param schemaName Schema name.
         * @return {@code this}.
         */
        public Builder schemaName(String schemaName) {
            params.schema = schemaName;
            return this;
        }

        /**
         * Sets index simple name.
         *
         * @param indexName Index simple name.
         * @return {@code this}.
         */
        public Builder indexName(String indexName) {
            params.indexName = indexName;
            return this;
        }

        /**
         * Builds parameters.
         *
         * @return Parameters.
         */
        public DropIndexParams build() {
            DropIndexParams params0 = params;
            params = null;
            return params0;
        }
    }
}
