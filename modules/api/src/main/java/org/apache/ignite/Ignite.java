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

package org.apache.ignite;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.catalog.IgniteCatalog;
import org.apache.ignite.catalog.Options;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.manager.IgniteTables;
import org.apache.ignite.tx.IgniteTransactions;

/**
 * Ignite API entry point.
 */
public interface Ignite extends AutoCloseable {
    /**
     * Returns the Ignite node name.
     *
     * @return Ignite node name.
     */
    String name();

    /**
     * Returns an object for manipulating the Ignite tables.
     *
     * @return Ignite tables.
     */
    IgniteTables tables();

    /**
     * Returns a transaction facade.
     *
     * @return Ignite transactions.
     */
    IgniteTransactions transactions();

    /**
     * Returns a facade for the SQL query engine.
     *
     * @return Ignite SQL facade.
     */
    IgniteSql sql();

    /**
     * Returns {@link IgniteCompute}, which can be used to execute compute jobs.
     *
     * @return compute management object
     * @see IgniteCompute
     * @see ComputeJob
     */
    IgniteCompute compute();

    /**
     * Gets the cluster nodes.
     * NOTE: Temporary API to enable Compute until we have proper Cluster API.
     *
     * @return Collection of cluster nodes.
     */
    Collection<ClusterNode> clusterNodes();

    /**
     * Returns cluster nodes.
     * NOTE: A temporary API to enable Compute until the permanent Cluster API becomes available.
     *
     * @return Collection of cluster nodes.
     */
    CompletableFuture<Collection<ClusterNode>> clusterNodesAsync();

    /**
     * Returns {@link IgniteCatalog}, which can be used to create and execute SQL DDL queries from annotated classes or from fluent-style
     * builders.
     *
     * @param options Query options.
     * @return Catalog object.
     */
    IgniteCatalog catalog(Options options);

    /**
     * Returns {@link IgniteCatalog} with default options, which can be used to create and execute SQL DDL queries from annotated classes or
     * from fluent-style builders.
     *
     * @return Catalog object.
     */
    default IgniteCatalog catalog() {
        return catalog(Options.DEFAULT);
    }
}
