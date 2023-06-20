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

import org.apache.ignite.compute.DeploymentUnit;

/**
 * Interface for accessing deployment units.
 */
public interface DeploymentUnitAccessor {
    /**
     * Acquires the deployment unit. Each call to this method must be matched with a call to {@link DisposableDeploymentUnit#release()}.
     * The acquired deployment unit must not be undeployed.
     *
     * @param unit Deployment unit.
     * @return Disposable deployment unit.
     */
    DisposableDeploymentUnit acquire(DeploymentUnit unit);

    /**
     * Checks if the deployment unit is acquired.
     *
     * @param unit Deployment unit.
     * @return {@code true} if the deployment unit is acquired.
     */
    boolean isAcquired(DeploymentUnit unit);
}
