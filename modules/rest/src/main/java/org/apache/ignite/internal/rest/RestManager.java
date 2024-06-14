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

package org.apache.ignite.internal.rest;

import static org.apache.ignite.internal.rest.PathAvailability.available;
import static org.apache.ignite.internal.rest.PathAvailability.unavailable;

import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Rest manager.
 */
public class RestManager {
    private static final IgniteLogger LOG = Loggers.forClass(RestManager.class);

    private static final String DURING_INITIALIZATION_TITLE = "REST temporarily unavailable";

    private static final String DURING_INITIALIZATION_REASON = "REST services are unavailable during cluster initialization";

    private static final String CLUSTER_NOT_INITIALIZED_TITLE = "Cluster is not initialized";

    private static final String CLUSTER_NOT_INITIALIZED_REASON = "Cluster is not initialized. "
            + "Use 'cluster init' command to initialize the cluster. "
            + "Example: cluster init --name=<clusterName> --metastorage-group=<node name>";

    private static final String[] DEFAULT_ENDPOINTS = {
            "/management/v1/configuration/node",
            "/management/v1/cluster/init",
            "/management/v1/cluster/topology/physical",
            "/management/v1/node"
    };

    private final String[] availableOnStartEndpoints;

    private RestState state = RestState.NOT_INITIALIZED;

    public RestManager() {
        this(DEFAULT_ENDPOINTS);
    }

    public RestManager(String[] availableOnStartEndpoints) {
        this.availableOnStartEndpoints = availableOnStartEndpoints;
    }

    /**
     * Returns path availability.
     *
     * @param requestPath Request path.
     * @return Path
     */
    public PathAvailability pathAvailability(String requestPath) {
        switch (state) {
            case INITIALIZED:
                return available();
            case INITIALIZATION:
                return unavailable(DURING_INITIALIZATION_TITLE, DURING_INITIALIZATION_REASON);
            case NOT_INITIALIZED:
                return pathDisabledForNotInitializedCluster(requestPath)
                        ? unavailable(CLUSTER_NOT_INITIALIZED_TITLE, CLUSTER_NOT_INITIALIZED_REASON)
                        : available();
            default:
                throw new IllegalStateException("Unrecognized state " + state);
        }
    }

    /**
     * Returns disabled or not path of REST method.
     *
     * @param path REST method path.
     * @return {@code true} in case when path disable or {@code false} if not.
     */
    private boolean pathDisabledForNotInitializedCluster(String path) {
        for (String enabledPath : availableOnStartEndpoints) {
            if (path.startsWith(enabledPath)) {
                return false;
            }
        }
        return true;
    }

    void setState(RestState state) {
        if (state.ordinal() <= this.state.ordinal()) {
            LOG.error("Incorrect state transfer from " + this.state + " to " + state);
            return;
        }
        this.state = state;
    }
}
