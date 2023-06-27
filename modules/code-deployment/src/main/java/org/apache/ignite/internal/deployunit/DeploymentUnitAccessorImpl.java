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

import java.util.function.Consumer;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.internal.util.RefCountedObjectPool;

/**
 * Implementation of {@link DeploymentUnitAccessor}.
 */
public class DeploymentUnitAccessorImpl implements DeploymentUnitAccessor {
    private final RefCountedObjectPool<DeploymentUnit, DisposableDeploymentUnit> pool = new RefCountedObjectPool<>();

    private final FileDeployerService deployer;

    public DeploymentUnitAccessorImpl(FileDeployerService deployer) {
        this.deployer = deployer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized DisposableDeploymentUnit acquire(DeploymentUnit unit) {
        return pool.acquire(unit, ignored -> new DisposableDeploymentUnit(
                        unit,
                        deployer.unitPath(unit.name(), unit.version(), true),
                        () -> pool.release(unit)
                )
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized boolean computeIfNotAcquired(DeploymentUnit unit, Consumer<DeploymentUnit> consumer) {
        if (pool.isAcquired(unit)) {
            return false;
        } else {
            consumer.accept(unit);
            return true;
        }
    }
}
