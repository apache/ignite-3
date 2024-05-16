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

package org.apache.ignite.internal.datareplication;

import static org.apache.ignite.internal.lang.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.manager.IgniteComponent;

/**
 * The main responsibilities of this class:
 * - Start the appropriate replication nodes (RAFT nodes at the moment) and logic replicas on the zone creation.
 * - Stop the same entities on the zone removing.
 * - Support the rebalance mechanism and start the new replication entities when the rebalance triggers occurred.
 */
public class ReplicaLifecycleManager implements IgniteComponent {

    /* Feature flag for zone based collocation track */
    // TODO IGNITE-22115 remove it
    public static boolean ENABLED = getBoolean("IGNITE_ZONE_BASED_REPLICATION", false);

    @Override
    public CompletableFuture<Void> startAsync() {
        if (!ENABLED) {
            return nullCompletedFuture();
        }

        // TODO: IGNITE-22231 Will be replaced by the real code.
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync() {
        if (!ENABLED) {
            return nullCompletedFuture();
        }

        // TODO: IGNITE-22231 Will be replaced by the real code.
        return nullCompletedFuture();
    }
}
