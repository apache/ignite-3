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

package org.apache.ignite.internal.app;

import java.util.concurrent.Executor;
import org.apache.ignite.Ignite;
import org.apache.ignite.catalog.IgniteCatalog;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.sql.api.IgniteSqlImpl;
import org.apache.ignite.internal.sql.api.JobScopedIgniteSql;
import org.apache.ignite.internal.sql.api.PublicApiThreadingIgniteSql;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.internal.tx.impl.PublicApiThreadingIgniteTransactions;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.network.IgniteCluster;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.tx.IgniteTransactions;

/**
 * A lightweight wrapper around {@link IgniteImpl} that scopes {@link #transactions()} and {@link #sql()} to a per-job
 * {@link HybridTimestampTracker}. This prevents one compute job from polluting another job's observable timestamp.
 */
class JobScopedIgnite implements Ignite, Wrapper {
    private final Ignite delegate;

    private final IgniteTransactions scopedTransactions;

    private final IgniteSql scopedSql;

    JobScopedIgnite(
            Ignite delegate,
            HybridTimestampTracker jobTracker,
            TxManager txManager,
            IgniteSqlImpl sql,
            Executor asyncContinuationExecutor
    ) {
        this.delegate = delegate;
        this.scopedTransactions = new PublicApiThreadingIgniteTransactions(
                new IgniteTransactionsImpl(txManager, jobTracker), asyncContinuationExecutor
        );
        this.scopedSql = new PublicApiThreadingIgniteSql(
                new JobScopedIgniteSql(sql, jobTracker), asyncContinuationExecutor
        );
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public IgniteTables tables() {
        return delegate.tables();
    }

    @Override
    public IgniteTransactions transactions() {
        return scopedTransactions;
    }

    @Override
    public IgniteSql sql() {
        return scopedSql;
    }

    @Override
    public IgniteCompute compute() {
        return delegate.compute();
    }

    @Override
    public IgniteCatalog catalog() {
        return delegate.catalog();
    }

    @Override
    public IgniteCluster cluster() {
        return delegate.cluster();
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return classToUnwrap.cast(delegate);
    }
}
