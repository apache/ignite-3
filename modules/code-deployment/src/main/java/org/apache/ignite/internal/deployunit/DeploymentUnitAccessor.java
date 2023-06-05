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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.version.Version;

/**
 * Interface for accessing deployment units.
 */
public interface DeploymentUnitAccessor {
    /**
     * Detects the latest version of the deployment unit.
     */
    CompletableFuture<Version> detectLatestDeployedVersion(String id);

    /**
     * Acquires the deployment unit. Each call to this method must be matched with a call to {@link DisposableDeploymentUnit#release()}.
     */
    CompletableFuture<DisposableDeploymentUnit> acquire(DeploymentUnit unit);

    boolean isAcquired(DeploymentUnit unit);
}
