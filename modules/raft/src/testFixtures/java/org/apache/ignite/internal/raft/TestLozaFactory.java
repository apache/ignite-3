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

import org.apache.ignite.internal.failure.NoOpFailureProcessor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
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
     * @param raftConfiguration Raft configuration.
     * @param clock A hybrid logical clock.
     */
    public static Loza create(
            ClusterService clusterNetSvc,
            RaftConfiguration raftConfiguration,
            HybridClock clock
    ) {
        return create(clusterNetSvc, raftConfiguration, clock, new RaftGroupEventsClientListener());
    }

    /**
     * Factory method for {@link Loza}.
     *
     * @param clusterNetSvc Cluster network service.
     * @param raftConfig Raft configuration.
     * @param clock A hybrid logical clock.
     * @param raftGroupEventsClientListener Raft event listener.
     */
    public static Loza create(
            ClusterService clusterNetSvc,
            RaftConfiguration raftConfig,
            HybridClock clock,
            RaftGroupEventsClientListener raftGroupEventsClientListener) {
        return new Loza(
                clusterNetSvc,
                new NoOpMetricManager(),
                raftConfig,
                clock,
                raftGroupEventsClientListener,
                new NoOpFailureProcessor()
        );
    }
}
