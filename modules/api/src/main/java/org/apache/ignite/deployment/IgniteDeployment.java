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

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.deployment.version.Version;

/**
 * Provides access to the Deployment Unit functionality.
 */
public interface IgniteDeployment {

    default CompletableFuture<Boolean> deploy(String id, DeploymentUnit deploymentUnit) {
        return deploy(id, Version.latest(), deploymentUnit);
    }

    /**
     * Deploy provided unit to current node.
     * After deploy finished, this deployment unit will be place to CMG group asynchronously.
     *
     * @param id Unit identifier.
     * @param version Unit version.
     * @param deploymentUnit Unit content.
     * @return Future with success or not result.
     */
    CompletableFuture<Boolean> deploy(String id, Version version, DeploymentUnit deploymentUnit);

    default CompletableFuture<Void> undeploy(String id) {
        return undeploy(id, Version.latest());
    }

    /**
     * Undeploy unit with corresponding identifier and version.
     * Note that unit files will be deleted asynchronously.
     *
     * @param id Unit identifier.
     * @param version Unit version.
     * @return Future completed when unit will be undeployed.
     */
    CompletableFuture<Void> undeploy(String id, Version version);

    /**
     * List of all deployed units identifiers.
     *
     * @return Future with result.
     */
    CompletableFuture<Set<UnitStatus>> list();

    /**
     * List with all deployed versions of required unit identifiers.
     *
     * @param unitId unit identifier.
     * @return Future with result.
     */
    CompletableFuture<Set<Version>> versions(String unitId);

    CompletableFuture<UnitStatus> status(String id);
}
