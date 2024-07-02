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

package org.apache.ignite.internal.deployunit.exception;

import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.deployunit.DeploymentStatus;
import org.apache.ignite.lang.ErrorGroups.CodeDeployment;
import org.apache.ignite.lang.IgniteException;

/**
 * Throws when trying to use unavailable unit for computing. Unit can be unavailable if it has one of the following statuses:
 * {@link DeploymentStatus#OBSOLETE} or {@link DeploymentStatus#REMOVING} or {@link DeploymentStatus#UPLOADING}.
 */
public class DeploymentUnitUnavailableException extends IgniteException {
    /**
     * Unit id.
     */
    private final String id;

    /**
     * Unit version.
     */
    private final Version version;

    /**
     * Cluster status.
     */
    private final DeploymentStatus clusterStatus;

    /**
     * Node status.
     */
    private final DeploymentStatus nodeStatus;

    /**
     * Constructor.
     *
     * @param id Unit id.
     * @param version Unit version.
     * @param clusterStatus Cluster status.
     * @param nodeStatus Node status.
     */
    public DeploymentUnitUnavailableException(String id, Version version, DeploymentStatus clusterStatus, DeploymentStatus nodeStatus) {
        super(CodeDeployment.UNIT_UNAVAILABLE_ERR, message(id, version, clusterStatus, nodeStatus));
        this.id = id;
        this.version = version;
        this.clusterStatus = clusterStatus;
        this.nodeStatus = nodeStatus;
    }

    public String id() {
        return id;
    }

    public Version version() {
        return version;
    }

    public DeploymentStatus clusterStatus() {
        return clusterStatus;
    }

    public DeploymentStatus nodeStatus() {
        return nodeStatus;
    }

    private static String message(String id, Version version, DeploymentStatus clusterStatus, DeploymentStatus nodeStatus) {
        return "Unit " + id + ":" + version + " is unavailable. "
                + "Cluster status: " + clusterStatus + ", node status: " + nodeStatus;
    }

}
