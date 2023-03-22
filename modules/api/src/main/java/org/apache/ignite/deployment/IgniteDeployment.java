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
     * Deploys the specified unit to the current node with the latest version.
     *
     * @param id Unit identifier. Not empty and not null.
     * @param deploymentUnit Unit content.
     * @return CompletableFuture (success or failure).
     */
    default CompletableFuture<Boolean> deployAsync(String id, DeploymentUnit deploymentUnit) {
        return deployAsync(id, Version.LATEST, deploymentUnit);
    }

    /**
     * Deploys the specified unit to the current node.
     * The deployment unit is placed in the CMG group asynchronously.
     *
     * @param id Unit identifier. Not empty and not null.
     * @param version Unit version.
     * @param deploymentUnit Unit content.
     * @return CompletableFuture (success or failure).
     */
    CompletableFuture<Boolean> deployAsync(String id, Version version, DeploymentUnit deploymentUnit);

    /**
     * Un-deploys the latest version of the specified unit.
     *
     * @param id Unit identifier. Not empty and not null.
     * @return CompletableFuture; 'failure' if the specified unit does not exist.
     */
    default CompletableFuture<Void> undeployAsync(String id) {
        return undeployAsync(id, Version.LATEST);
    }

    /**
     * Un-deploys the unit specified by the identifier and version.
     * The unit files are deleted asynchronously.
     *
     * @param id Unit identifier.
     * @param version Unit version.
     * @return CompletableFuture; 'failure' if the specified unit does not exist.
     */
    CompletableFuture<Void> undeployAsync(String id, Version version);

    /**
     * Lists all deployed units.
     *
     * @return CompletableFuture with the list.
     */
    CompletableFuture<List<UnitStatus>> unitsAsync();

    /**
     * Lists all deployed versions of the specified unit.
     *
     * @param id Unit identifier. Not empty and not null.
     * @return CompletableFuture with the version list.
     *      If the specified unit does not exist, the list is empty.
     */
    CompletableFuture<List<Version>> versionsAsync(String id);

    /**
     * Returns status of the specified unit.
     *
     * @param id Unit identifier. Not empty and not null.
     * @return CompletableFuture with the unit status.
     *      'Failure' if the specified unit does not exist.
     */
    CompletableFuture<UnitStatus> statusAsync(String id);

    /**
     * Returns list with deployed units on node with provided consistent id.
     *
     * @param consistentId Node consistent id.
     * @return List with deployed units on node with provided consistent id.
     */
    CompletableFuture<List<UnitStatus>> findUnitByConsistentIdAsync(String consistentId);
}
