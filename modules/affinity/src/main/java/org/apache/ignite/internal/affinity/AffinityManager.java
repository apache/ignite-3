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
import org.apache.ignite.internal.affinity.event.AffinityEvent;
import org.apache.ignite.internal.affinity.event.AffinityEventParameters;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.manager.Producer;
import org.apache.ignite.network.ClusterNode;

/**
 * Affinity manager is responsible for affinity function related logic including calculating affinity assignments.
 */
// TODO sanpwc: Consider renaming to AffinityService or <some-better-name>
public class AffinityManager extends Producer<AffinityEvent, AffinityEventParameters> implements IgniteComponent {
    /** Baseline manager. */
    private final BaselineManager baselineMgr;

    /**
     * Creates a new affinity manager.
     *
     * @param baselineMgr Baseline manager.
     */
    public AffinityManager(
        BaselineManager baselineMgr
    ) {
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

    // TODO sanpwc: javadoc
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
