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

package org.apache.ignite.internal.cluster.management.events;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.event.EventParameters;
import org.jetbrains.annotations.Nullable;

/** Transparent data container for the {@link ClusterManagerGroupEvent#BEFORE_START_RAFT_GROUP}. */
public class BeforeStartRaftGroupEventParameters implements EventParameters {
    private final Set<String> nodeNames;
    private final @Nullable String initialClusterConfig;

    /** Constructor. */
    public BeforeStartRaftGroupEventParameters(Set<String> nodeNames, @Nullable String initialClusterConfig) {
        this.nodeNames = new HashSet<>(nodeNames);
        this.initialClusterConfig = initialClusterConfig;
    }

    public Set<String> nodeNames() {
        return nodeNames;
    }

    @Nullable
    public String initialClusterConfig() {
        return initialClusterConfig;
    }
}
