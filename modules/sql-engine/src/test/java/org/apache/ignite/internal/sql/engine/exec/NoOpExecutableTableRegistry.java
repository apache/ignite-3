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

package org.apache.ignite.internal.sql.engine.exec;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.table.InternalTable;

/** Stub implementation for {@link ExecutableTableRegistry}. */
public final class NoOpExecutableTableRegistry implements ExecutableTableRegistry {

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<ExecutableTable> getTable(int tableId, TableDescriptor tableDescriptor) {
        return CompletableFuture.completedFuture(new NoOpExecutableTable(tableId));
    }

    private static final class NoOpExecutableTable implements ExecutableTable {

        private final int tableId;

        private NoOpExecutableTable(int tableId) {
            this.tableId = tableId;
        }

        /** {@inheritDoc} */
        @Override
        public InternalTable table() {
            throw noDependency();
        }

        /** {@inheritDoc} */
        @Override
        public UpdateableTable updates() {
            throw noDependency();
        }

        /** {@inheritDoc} */
        @Override
        public TableRowConverter rowConverter() {
            throw noDependency();
        }

        private IllegalStateException noDependency() {
            return new IllegalStateException("NoOpExecutableTable: " + tableId);
        }
    }
}
