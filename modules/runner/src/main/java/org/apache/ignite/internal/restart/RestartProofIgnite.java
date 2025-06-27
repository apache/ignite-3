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

import org.apache.ignite.Ignite;
import org.apache.ignite.catalog.IgniteCatalog;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.network.IgniteCluster;
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
    private final IgniteSql sql;
    private final IgniteCompute compute;
    private final IgniteCatalog catalog;
    private final RestartProofIgniteCluster cluster;

    /**
     * Constructor.
     */
    public RestartProofIgnite(IgniteAttachmentLock attachmentLock) {
        this.attachmentLock = attachmentLock;

        tables = new RestartProofIgniteTables(attachmentLock);
        transactions = new RestartProofIgniteTransactions(attachmentLock);
        sql = new RestartProofIgniteSql(attachmentLock);
        compute = new RestartProofIgniteCompute(attachmentLock);
        catalog = new RestartProofIgniteCatalog(attachmentLock);
        cluster = new RestartProofIgniteCluster(attachmentLock);
    }

    @Override
    public String name() {
        return attachmentLock.attached(Ignite::name);
    }

    @Override
    public IgniteTables tables() {
        return tables;
    }

    @Override
    public IgniteTransactions transactions() {
        return transactions;
    }

    @Override
    public IgniteSql sql() {
        return sql;
    }

    @Override
    public IgniteCompute compute() {
        return compute;
    }

    @Override
    public IgniteCatalog catalog() {
        return catalog;
    }

    @Override
    public IgniteCluster cluster() {
        return cluster;
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return attachmentLock.attached(classToUnwrap::cast);
    }
}
