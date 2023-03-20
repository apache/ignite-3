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

import java.util.Collections;
import java.util.Set;

/**
 * ALTER TABLE ... DROP COLUMN statement.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class AlterTableDropColumnParams extends AbstractTableCommandParams {
    private static final long serialVersionUID = 4111208774796975682L;
    /** Quietly ignore this command if column is not exist. */
    private boolean ifColumnExists;

    /** Columns. */
    private Set<String> cols;

    public Set<String> columns() {
        return Collections.unmodifiableSet(cols);
    }

    /**
     * Exists flag.
     *
     * @return Quietly ignore this command if column is not exist.
     */
    public boolean ifColumnExists() {
        return ifColumnExists;
    }

    /**
     * Parameters builder.
     */
    public static class Builder extends AbstractTableCommandParams.AbstractBuilder<AlterTableDropColumnParams, Builder> {
        private Builder() {
            super(new AlterTableDropColumnParams());
        }

        /**
         * Columns to drop.
         *
         * @param cols Columns.
         * @return {@code this}.
         */
        public Builder columns(Set<String> cols) {
            params.cols = cols;
            return this;
        }

        /**
         * Set exists flag.
         *
         * @param ifColumnExists Quietly ignore this command if column is not exist.
         * @return {@code this}.
         */
        public Builder ifColumnExists(boolean ifColumnExists) {
            params.ifColumnExists = ifColumnExists;
            return this;
        }
    }
}
