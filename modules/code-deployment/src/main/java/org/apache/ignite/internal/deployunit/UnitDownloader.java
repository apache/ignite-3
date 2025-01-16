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

package org.apache.ignite.internal.deployunit;

import java.util.Collection;
import org.apache.ignite.deployment.version.Version;

/**
 * Unit downloader.
 */
@FunctionalInterface
interface UnitDownloader {
    /**
     * Downloads specified unit from any node from the specified collection of nodes to the local node, deploys it and sets the node status
     * to {@link DeploymentStatus#DEPLOYED}.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @param nodes Nodes where the unit is deployed.
     */
    void downloadUnit(String id, Version version, Collection<String> nodes);
}
