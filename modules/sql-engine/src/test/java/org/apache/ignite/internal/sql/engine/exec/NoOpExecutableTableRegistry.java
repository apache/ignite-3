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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.schema.PartitionCalculator;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;

/** Stub implementation for {@link ExecutableTableRegistry}. */
public final class NoOpExecutableTableRegistry implements ExecutableTableRegistry {

    private static final IgniteLogger LOG = Loggers.forClass(NoOpExecutableTableRegistry.class);

    private volatile Duration delay = Duration.ZERO;

    /** Sets a delay for {@link #getTable(int, int)} operation. */
    public void setGetTableDelay(Duration delay) {
        this.delay = Objects.requireNonNull(delay, "delay");
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<ExecutableTable> getTable(int catalogVersion, int tableId) {
        Duration delay = this.delay;

        CompletableFuture<ExecutableTable> f = new CompletableFuture<>();

        LOG.info("Requested tableId={} in catalogVersion={} with delay={}", tableId, catalogVersion, delay);

        f.completeOnTimeout(new NoOpExecutableTable(tableId), delay.toMillis(), TimeUnit.MILLISECONDS).thenRun(() -> {
            LOG.info("Return tableId={} in catalogVersion={} after delay={}", tableId, catalogVersion, delay);
        });

        return f;
    }

    private static final class NoOpExecutableTable implements ExecutableTable {

        private final int tableId;

        private NoOpExecutableTable(int tableId) {
            this.tableId = tableId;
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
        public TableDescriptor tableDescriptor() {
            throw noDependency();
        }

        @Override
        public Supplier<PartitionCalculator> partitionCalculator() {
            throw new UnsupportedOperationException();
        }

        private IllegalStateException noDependency() {
            return new IllegalStateException("NoOpExecutableTable: " + tableId);
        }
    }
}
