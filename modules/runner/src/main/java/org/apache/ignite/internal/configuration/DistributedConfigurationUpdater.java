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

package org.apache.ignite.internal.configuration;

import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.configuration.presentation.HoconPresentation;
import org.apache.ignite.internal.manager.IgniteComponent;

public class DistributedConfigurationUpdater implements IgniteComponent {
    private final ClusterManagementGroupManager cmgMgr;

    private final ConfigurationManager clusterCfgMgr;

    public DistributedConfigurationUpdater(ClusterManagementGroupManager cmgMgr, ConfigurationManager clusterCfgMgr) {
        this.cmgMgr = cmgMgr;
        this.clusterCfgMgr = clusterCfgMgr;
    }

    @Override
    public void start() {
        HoconPresentation presentation = new HoconPresentation(clusterCfgMgr.configurationRegistry());
        cmgMgr.clusterConfigurationToUpdate().thenAccept(action -> {
            if (action.currentAction() != null) {
                presentation.update(action.currentAction())
                        .thenApply(v -> action.nextAction());
            }
        });
    }

    @Override
    public void stop() throws Exception {
    }
}
