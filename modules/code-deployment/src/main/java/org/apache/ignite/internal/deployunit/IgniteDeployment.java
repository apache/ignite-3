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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.deployunit.structure.UnitFolder;
import org.apache.ignite.internal.manager.IgniteComponent;

/**
 * Provides access to the Deployment Unit functionality.
 */
public interface IgniteDeployment extends IgniteComponent {
    /**
     * Deploys provided unit to the current node. After the deploy is finished, the unit will be placed to the CMG group, if
     * {@code deployMode} is {@code MAJORITY}, or to all available units, if {@code deployMode} is {@code ALL} asynchronously.
     *
     * @param id Unit identifier. Not empty and not null.
     * @param version Unit version.
     * @param deploymentUnit Unit content.
     * @param nodesToDeploy Nodes for initial deploy.
     * @return Future with success or not result.
     */
    default CompletableFuture<Boolean> deployAsync(
            String id,
            Version version,
            DeploymentUnit deploymentUnit,
            NodesToDeploy nodesToDeploy
    ) {
        return deployAsync(id, version, false, deploymentUnit, nodesToDeploy);
    }

    /**
     * Deploys provided unit to the current node. After the deploy is finished, the unit will be placed to the CMG group, if
     * {@code deployMode} is {@code MAJORITY}, or to all available units, if {@code deployMode} is {@code ALL} asynchronously.
     *
     * @param id Unit identifier. Not empty and not null.
     * @param version Unit version.
     * @param force Force redeploy if unit with provided id and version exists.
     * @param deploymentUnit Unit content.
     * @param nodesToDeploy Nodes for initial deploy.
     * @return Future with success or not result.
     */
    CompletableFuture<Boolean> deployAsync(
            String id,
            Version version,
            boolean force,
            DeploymentUnit deploymentUnit,
            NodesToDeploy nodesToDeploy
    );

    /**
     * Undeploys unit with corresponding identifier and version.
     * Note that unit files will be deleted asynchronously.
     *
     * @param id Unit identifier.
     * @param version Unit version.
     * @return Future completed when unit will be undeployed.
     *      In case when specified unit not exist future will be failed.
     */
    CompletableFuture<Boolean> undeployAsync(String id, Version version);

    /**
     * Lists all units statuses.
     *
     * @return Future with the list of unit statuses.
     */
    CompletableFuture<List<UnitStatuses>> clusterStatusesAsync();

    /**
     * Lists all versions of the unit.
     *
     * @param id Unit identifier.
     * @return Future with the unit statuses. Result of the future can be null when the specified unit does not exist.
     */
    CompletableFuture<UnitStatuses> clusterStatusesAsync(String id);

    /**
     * Gets unit status of particular version.
     *
     * @param id Unit identifier.
     * @param version Unit version.
     * @return Future with unit status.
     */
    CompletableFuture<DeploymentStatus> clusterStatusAsync(String id, Version version);

    /**
     * Lists all deployed versions of the specified unit.
     *
     * @param id Unit identifier. Not empty and not null.
     * @return Future with list of all available version of unit. In case when unit with specified identifier not exist future list will be
     *         empty.
     */
    CompletableFuture<List<Version>> versionsAsync(String id);

    /**
     * Lists all units statuses on this node.
     *
     * @return Future with the list of unit statuses.
     */
    CompletableFuture<List<UnitStatuses>> nodeStatusesAsync();

    /**
     * Returns status of unit with provided identifier.
     *
     * @param id Unit identifier.
     * @return Future with the unit statuses.
     */
    CompletableFuture<UnitStatuses> nodeStatusesAsync(String id);

    /**
     * Gets unit status of particular version on this node.
     *
     * @param id Unit identifier.
     * @param version Unit version.
     * @return Future with unit status.
     */
    CompletableFuture<DeploymentStatus> nodeStatusAsync(String id, Version version);

    /**
     * Requests on demand deploy to local node unit with provided identifier and version.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @return {@code true} if unit already deployed or deployed successfully.
     *      {@code false} if deploy failed or unit with provided identifier and version doesn't exist.
     */
    CompletableFuture<Boolean> onDemandDeploy(String id, Version version);

    /**
     * Detects the latest version of the deployment unit.
     *
     * @param id Deployment unit identifier.
     * @return Future with the latest version of the deployment unit.
     */
    CompletableFuture<Version> detectLatestDeployedVersion(String id);

    /**
     * Returns unit content tree structure.
     *
     * @param unitId Deployment unit identifier.
     * @param version Deployment unit version.
     */
    CompletableFuture<UnitFolder> nodeUnitFileStructure(String unitId, Version version);
}
