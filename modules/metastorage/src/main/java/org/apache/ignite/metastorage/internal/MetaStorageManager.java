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

package org.apache.ignite.metastorage.internal;

import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkHandlersProvider;
import org.apache.ignite.network.NetworkMessageHandler;
import org.apache.ignite.raft.internal.RaftManager;

public class MetaStorageManager {
    private final NetworkCluster network;

    private final RaftManager raftMgr;

    private final ConfigurationManager locConfigurationMgr;

    public MetaStorageManager(
        NetworkCluster network,
        RaftManager raftMgr,
        ConfigurationManager locConfigurationMgr)
    {
        this.network = network;
        this.raftMgr = raftMgr;
        this.locConfigurationMgr = locConfigurationMgr;

        network.addHandlersProvider(new NetworkHandlersProvider() {
            @Override public NetworkMessageHandler messageHandler() {
                return (message, sender, corellationId) -> {
                    // TODO sanpwc: Add MetaStorageMessageTypes.CLUSTER_INIT_REQUEST message handler.
                };
            }
        });
    }

    public void registerWatch() {

    }

    public void deployWatches() {

    }
}

