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
import org.apache.ignite.internal.sql.engine.metadata.ColocationGroup;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;

/** Test implementation of {@link ExecutableTableRegistry}. */
public final class TestExecutableTableRegistry implements ExecutableTableRegistry {

    private volatile ColocationGroupProvider colocationGroupProvider;

    /** Constructor. */
    public TestExecutableTableRegistry() {
        this.colocationGroupProvider = (tableId) ->
                CompletableFuture.failedFuture(new IllegalStateException("No result for fetch replicas"));
    }

    /** Sets a function to be called for {@link ExecutableTable#fetchColocationGroup()}. */
    public void setColocatioGroupProvider(ColocationGroupProvider groupProvider) {
        assert groupProvider != null;
        this.colocationGroupProvider = groupProvider;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<ExecutableTable> getTable(int tableId, TableDescriptor tableDescriptor) {
        return CompletableFuture.completedFuture(new TestExecutableTable(tableId, colocationGroupProvider));
    }

    @Override
    public CompletableFuture<ExecutableTable> getTable(int tableId, String tableName, TableDescriptor tableDescriptor) {
        return CompletableFuture.completedFuture(new TestExecutableTable(tableId, colocationGroupProvider));
    }

    private static final class TestExecutableTable implements ExecutableTable {

        private final int tableId;

        private final ColocationGroupProvider fetchReplicas;

        private TestExecutableTable(int tableId, ColocationGroupProvider fetchReplicas) {
            this.tableId = tableId;
            this.fetchReplicas = fetchReplicas;
        }

        /** {@inheritDoc} */
        @Override
        public ScannableTable scannableTable() {
            throw noDependency();
        }

        /** {@inheritDoc} */
        @Override
        public UpdatableTable updatableTable() {
            throw noDependency();
        }

        /** {@inheritDoc} */
        @Override
        public CompletableFuture<ColocationGroup> fetchColocationGroup() {
            return fetchReplicas.getGroup(tableId);
        }

        private IllegalStateException noDependency() {
            return new IllegalStateException("NoOpExecutableTable: " + tableId);
        }
    }

    /** Provides colocation groups. */
    @FunctionalInterface
    public interface ColocationGroupProvider {

        /** Retrieves colocation group for a table with the given id. */
        CompletableFuture<ColocationGroup> getGroup(int tableId);
    }
}
