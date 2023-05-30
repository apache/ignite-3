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
import org.apache.ignite.internal.catalog.descriptors.ColumnCollation;
import org.jetbrains.annotations.Nullable;

/**
 * CREATE INDEX statement.
 */
public class CreateIndexParams extends AbstractIndexCommandParams {
    /** Creates parameters builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** Type of the index to create. */
    public enum Type {
        SORTED, HASH
    }

    /** Table name. */
    private String tableName;

    /** Index type. */
    private Type type;

    /** Indexed columns. */
    private List<String> columns;

    /** Columns collations. */
    private @Nullable List<ColumnCollation> collations;

    /**
     * Gets index type.
     */
    public Type type() {
        return type;
    }

    /**
     * Gets table name.
     */
    public String tableName() {
        return tableName;
    }

    /**
     * Gets indexed columns.
     */
    public List<String> columns() {
        return columns;
    }

    /**
     * Gets columns collations.
     */
    public @Nullable List<ColumnCollation> collations() {
        return collations;
    }

    /**
     * Parameters builder.
     */
    public static class Builder extends AbstractIndexCommandParams.AbstractBuilder<CreateIndexParams, CreateIndexParams.Builder> {
        private Builder() {
            super(new CreateIndexParams());
        }

        /**
         * Set index type.
         *
         * @param type Index type.
         * @return {@code this}.
         */
        public Builder type(Type type) {
            params.type = type;

            return this;
        }

        /**
         * Set table name.
         *
         * @param tableName Table name.
         * @return {@code this}.
         */
        public Builder tableName(String tableName) {
            params.tableName = tableName;

            return this;
        }

        /**
         * Set columns names.
         *
         * @param columns Columns names.
         * @return {@code this}.
         */
        public Builder columns(List<String> columns) {
            params.columns = columns;

            return this;
        }

        /**
         * Set columns collations.
         *
         * @param collations Columns collations.
         * @return {@code this}.
         */
        public Builder collations(@Nullable List<ColumnCollation> collations) {
            params.collations = collations;

            return this;
        }
    }
}
