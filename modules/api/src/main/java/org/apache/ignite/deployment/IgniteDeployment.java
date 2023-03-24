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

package org.apache.ignite.deployment;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.deployment.version.Version;

/**
 * Provides access to the Deployment Unit functionality.
 */
public interface IgniteDeployment {
    /**
     * Deploys a specified unit to the current node with the latest version.
     *
     * @param id Unit identifier. Not empty and not null.
     * @param deploymentUnit Unit content.
     * @return Future with the operation result.
     */
    default CompletableFuture<Boolean> deployAsync(String id, DeploymentUnit deploymentUnit) {
        return deployAsync(id, Version.LATEST, deploymentUnit);
    }

    /**
     * Deploys a specified unit to the current node.
     * Upon deployment, the unit is placed in the CMG group asynchronously.
     *
     * @param id Unit identifier. Not empty and not null.
     * @param version Unit version.
     * @param deploymentUnit Unit content.
     * @return Future with the operation result.
     */
    CompletableFuture<Boolean> deployAsync(String id, Version version, DeploymentUnit deploymentUnit);

    /**
     * Un-deploys the latest version of a specified unit.
     *
     * @param id Unit identifier. Not empty and not null.
     * @return Future completed the unit is un-deployed.
     *      'Failed' if the specified unit does not exist.
     */
    default CompletableFuture<Void> undeployAsync(String id) {
        return undeployAsync(id, Version.LATEST);
    }

    /**
     * Un-deploys a unit specified by an identifier and a version.
     * the unit files are deleted asynchronously.
     *
     * @param id Unit identifier.
     * @param version Unit version.
     * @return Future completed when the unit is un-deployed.
     *      'Failed' if the specified unit does not exist.
     */
    CompletableFuture<Void> undeployAsync(String id, Version version);

    /**
     * Lists all deployed units.
     *
     * @return Future with a list.
     */
    CompletableFuture<List<UnitStatus>> unitsAsync();

    /**
     * Lists all deployed versions of a specified unit.
     *
     * @param id Unit identifier. Not empty and not null.
     * @return Future with a list of all available version of the unit.
     *      If the specified unit does not exist, the list is empty.
     */
    CompletableFuture<List<Version>> versionsAsync(String id);

    /**
     * Returns a status of a unit specified by its identifier.
     *
     * @param id Unit identifier. Not empty and not null.
     * @return Future with the unit status.
     *      'Failed' if the specified unit does not exist.
     */
    CompletableFuture<UnitStatus> statusAsync(String id);

    /**
     * Returns a list of units deployed to a node specified by its consistent ID.
     *
     * @param consistentId Node consistent id.
     * @return List of units deployed to the specified node.
     */
    CompletableFuture<List<UnitStatus>> findUnitByConsistentIdAsync(String consistentId);
}
