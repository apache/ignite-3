/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.affinity;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.affinity.event.AffinityEvent;
import org.apache.ignite.internal.affinity.event.AffinityEventParameters;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.manager.Producer;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.Conditions;
import org.apache.ignite.internal.metastorage.client.EntryEvent;
import org.apache.ignite.internal.metastorage.client.Operations;
import org.apache.ignite.internal.metastorage.client.WatchEvent;
import org.apache.ignite.internal.metastorage.client.WatchListener;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.NotNull;

/**
 * Affinity manager is responsible for affinity function related logic including calculating affinity assignments.
 */
public class AffinityManager extends Producer<AffinityEvent, AffinityEventParameters> implements IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(AffinityManager.class);

    /** Internal prefix for the metasorage. */
    private static final String INTERNAL_PREFIX = "internal.tables.assignment.";

    /**
     * MetaStorage manager in order to watch private distributed affinity specific configuration, cause
     * ConfigurationManger handles only public configuration.
     */
    private final MetaStorageManager metaStorageMgr;

    /** Configuration manager in order to handle and listen affinity specific configuration. */
    private final ConfigurationManager configurationMgr;

    /** Baseline manager. */
    private final BaselineManager baselineMgr;

    /**
     * Creates a new affinity manager.
     *
     * @param configurationMgr Configuration module.
     * @param metaStorageMgr Meta storage service.
     * @param baselineMgr Baseline manager.
     */
    public AffinityManager(
        ConfigurationManager configurationMgr,
        MetaStorageManager metaStorageMgr,
        BaselineManager baselineMgr
    ) {
        this.configurationMgr = configurationMgr;
        this.metaStorageMgr = metaStorageMgr;
        this.baselineMgr = baselineMgr;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        // TODO: 30.08.21 Affinity calc reassignment should go here.
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        // TODO: IGNITE-15161 Implement component's stop.
    }

    /**
     * Calculates an assignment for a table which was specified by id.
     *
     * @param tblId Table identifier.
     * @param tblName Table name.
     * @return A future which will complete when the assignment is calculated.
     */
    public List<List<ClusterNode>> calculateAssignments(int partitions, int replicas) {
        return RendezvousAffinityFunction.assignPartitions(
            baselineMgr.nodes(),
            partitions,
            replicas,
            false,
            null
        );
    }
}
