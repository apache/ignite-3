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

package org.apache.ignite.internal.restart;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.catalog.IgniteCatalog;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.tx.IgniteTransactions;

/**
 * Reference to a swappable {@link Ignite} instance. When a restart happens, this switches to the new Ignite instance.
 *
 * <p>API operations on this are linearized with respect to node restarts. Normally (except for situations when timeouts trigger), user
 * operations will not interact with detached objects.
 */
public class RestartProofIgnite implements Ignite, Wrapper {
    private final IgniteAttachmentLock attachmentLock;

    private final IgniteTables tables;
    private final IgniteTransactions transactions;

    /**
     * Constructor.
     */
    public RestartProofIgnite(IgniteAttachmentLock attachmentLock) {
        this.attachmentLock = attachmentLock;

        tables = new RestartProofIgniteTables(attachmentLock);
        transactions = new RestartProofIgniteTransactions(attachmentLock);
    }

    @Override
    public String name() {
        return attachmentLock.attached(Ignite::name);
    }

    @Override
    public IgniteTables tables() {
        return attachmentLock.attached(ignite -> tables);
    }

    @Override
    public IgniteTransactions transactions() {
        return attachmentLock.attached(ignite -> transactions);
    }

    @Override
    public IgniteSql sql() {
        // TODO: IGNITE-23013 - add a wrapper.
        return attachmentLock.attached(Ignite::sql);
    }

    @Override
    public IgniteCompute compute() {
        // TODO: IGNITE-23014 - add a wrapper.
        return attachmentLock.attached(Ignite::compute);
    }

    @Override
    public Collection<ClusterNode> clusterNodes() {
        return attachmentLock.attached(Ignite::clusterNodes);
    }

    @Override
    public CompletableFuture<Collection<ClusterNode>> clusterNodesAsync() {
        return attachmentLock.attachedAsync(Ignite::clusterNodesAsync);
    }

    @Override
    public IgniteCatalog catalog() {
        // TODO: IGNITE-23015 - add a wrapper.
        return attachmentLock.attached(Ignite::catalog);
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return attachmentLock.attached(classToUnwrap::cast);
    }
}
