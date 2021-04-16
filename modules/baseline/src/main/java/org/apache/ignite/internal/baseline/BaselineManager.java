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

package org.apache.ignite.internal.baseline;

import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.network.ClusterService;

/**
 * Baseline manager is responsible for handling baseline related logic.
 */
// TODO: IGNITE-14586 Remove @SuppressWarnings when implementation provided.
@SuppressWarnings({"FieldCanBeLocal", "unused"}) public class BaselineManager {
    /** Configuration manager in order to handle and listen baseline specific configuration.*/
    private final ConfigurationManager configurationMgr;

    /**
     * MetaStorage manager in order to watch private distributed baseline specific configuration,
     * cause ConfigurationManger handles only public configuration.
     */
    private final MetaStorageManager metastorageMgr;

    /** Cluster network service in order to retrieve information about current network members. */
    private final ClusterService clusterSvc;

    /**
     * Constructor.
     *
     * @param configurationMgr Configuration manager.
     * @param metastorageMgr MetaStorage manager.
     * @param clusterSvc Cluster network service.
     */
    public BaselineManager(
        ConfigurationManager configurationMgr,
        MetaStorageManager metastorageMgr,
        ClusterService clusterSvc
    ) {
        this.configurationMgr = configurationMgr;
        this.metastorageMgr = metastorageMgr;
        this.clusterSvc = clusterSvc;
    }
}

