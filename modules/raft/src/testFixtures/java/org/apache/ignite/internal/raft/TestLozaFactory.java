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

package org.apache.ignite.internal.raft;

import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.impl.GroupStoragesContextResolver;
import org.apache.ignite.internal.raft.storage.GroupStoragesDestructionIntents;
import org.apache.ignite.internal.raft.storage.impl.StorageDestructionIntent;
import org.apache.ignite.internal.raft.storage.impl.StoragesDestructionContext;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;

/** Utilities for creating {@link Loza} instances. */
public class TestLozaFactory {
    private TestLozaFactory() {
        // Intentionally left blank.
    }

    /**
     * Factory method for {@link Loza}.
     *
     * @param clusterNetSvc Cluster network service.
     * @param raftConfig Raft configuration.
     * @param systemLocalConfig Local system configuration.
     * @param clock A hybrid logical clock.
     */
    public static Loza create(
            ClusterService clusterNetSvc,
            RaftConfiguration raftConfig,
            SystemLocalConfiguration systemLocalConfig,
            HybridClock clock
    ) {
        return create(clusterNetSvc, raftConfig, systemLocalConfig, clock, new RaftGroupEventsClientListener());
    }

    /**
     * Factory method for {@link Loza}.
     *
     * @param clusterNetSvc Cluster network service.
     * @param raftConfig Raft configuration.
     * @param systemLocalConfig Local system configuration.
     * @param clock A hybrid logical clock.
     * @param raftGroupEventsClientListener Raft event listener.
     */
    public static Loza create(
            ClusterService clusterNetSvc,
            RaftConfiguration raftConfig,
            SystemLocalConfiguration systemLocalConfig,
            HybridClock clock,
            RaftGroupEventsClientListener raftGroupEventsClientListener
    ) {
        return new Loza(
                clusterNetSvc,
                new NoOpMetricManager(),
                raftConfig,
                systemLocalConfig,
                clock,
                raftGroupEventsClientListener,
                new NoOpFailureManager()
        );
    }

    /**
     * Factory method for {@link Loza}.
     *
     * @param clusterNetSvc Cluster network service.
     * @param raftConfig Raft configuration.
     * @param systemLocalConfig Local system configuration.
     * @param clock A hybrid logical clock.
     * @param raftGroupEventsClientListener Raft event listener.
     * @param groupStoragesDestructionIntents Storage to persist {@link StorageDestructionIntent}s.
     * @param groupStoragesContextResolver Resolver to get {@link StoragesDestructionContext}s for storage destruction.
     */
    public static Loza create(
            ClusterService clusterNetSvc,
            RaftConfiguration raftConfig,
            SystemLocalConfiguration systemLocalConfig,
            HybridClock clock,
            RaftGroupEventsClientListener raftGroupEventsClientListener,
            GroupStoragesDestructionIntents groupStoragesDestructionIntents,
            GroupStoragesContextResolver groupStoragesContextResolver
    ) {
        return new Loza(
                clusterNetSvc,
                new NoOpMetricManager(),
                raftConfig,
                systemLocalConfig,
                clock,
                raftGroupEventsClientListener,
                new NoOpFailureManager(),
                groupStoragesDestructionIntents,
                groupStoragesContextResolver
        );
    }
}
