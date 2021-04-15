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

package org.apache.ignite.baseline.internal;

import java.util.Collection;
import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.metastorage.internal.MetaStorageManager;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkMember;

public class BaselineManager {
    private final ConfigurationManager configurationMgr;

    private final MetaStorageManager metastorageMgr;

    private final NetworkCluster networkCluster;

    public BaselineManager(ConfigurationManager configurationMgr, MetaStorageManager metastorageMgr, NetworkCluster networkCluster) {
        this.configurationMgr = configurationMgr;
        this.metastorageMgr = metastorageMgr;
        this.networkCluster = networkCluster;
    }

    /**
     * Gets all nodes which participant in baseline and may process user data.
     *
     * @return All members which were in baseline.
     */
    public Collection<NetworkMember> nodes() {
        return networkCluster.allMembers();
    }
}

