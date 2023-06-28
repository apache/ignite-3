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
 * CREATE INDEX statement.
 */
public class CreateHashIndexParams extends AbstractIndexCommandParams {
    /** Creates parameters builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** Indexed columns. */
    private List<String> columns;

    /**
     * Gets indexed columns.
     */
    public List<String> columns() {
        return columns;
    }

    /**
     * Parameters builder.
     */
    public static class Builder extends AbstractIndexCommandParams.AbstractBuilder<CreateHashIndexParams, CreateHashIndexParams.Builder> {
        private Builder() {
            super(new CreateHashIndexParams());
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
    }
}
