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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Rest manager.
 */
public class RestManager {
    private static final IgniteLogger LOG = Loggers.forClass(RestManager.class);

    private static final String[] DEFAULT_ENDPOINTS = {
            "/management/v1/configuration/node",
            "/management/v1/cluster/init",
            "/management/v1/cluster/topology/physical",
            "/management/v1/node"
    };

    private final String[] availableOnStartEndpoints;

    private final AtomicBoolean isRestEnabled = new AtomicBoolean(true);

    public RestManager() {
        this(DEFAULT_ENDPOINTS);
    }

    public RestManager(String[] availableOnStartEndpoints) {
        this.availableOnStartEndpoints = availableOnStartEndpoints;
    }

    /**
     * Enable or disable whole REST component.
     *
     * @param isEnabled Is REST component enabled.
     */
    public void enabled(boolean isEnabled) {
        if (!isRestEnabled.compareAndSet(!isEnabled, isEnabled)) {
            String action = isEnabled ? "enable" : "disable";
            LOG.warn("Tried to " + action + " already " + action + "d REST component.");
        }
    }

    /**
     * Returns enabled or not REST component.
     *
     * @return Enabled or not REST component.
     */
    public boolean isRestEnabled() {
        return isRestEnabled.get();
    }

    /**
     * Returns disabled or not path of REST method.
     *
     * @param path REST method path.
     * @return {@code true} in case when path disable or {@code false} if not.
     */
    public boolean pathDisabledForNotInitializedCluster(String path) {
        for (String enabledPath : availableOnStartEndpoints) {
            if (path.startsWith(enabledPath)) {
                return false;
            }
        }
        return true;
    }
}
