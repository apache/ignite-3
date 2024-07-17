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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.util.RefCountedObjectPool;

/**
 * Implementation of {@link DeploymentUnitAccessor}.
 */
public class DeploymentUnitAccessorImpl implements DeploymentUnitAccessor {
    private final RefCountedObjectPool<DeploymentUnit, DisposableDeploymentUnit> pool = new RefCountedObjectPool<>();

    private final RefCountedObjectPool<DeploymentUnit, Lock> locks = new RefCountedObjectPool<>();

    private final FileDeployerService deployer;

    public DeploymentUnitAccessorImpl(FileDeployerService deployer) {
        this.deployer = deployer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DisposableDeploymentUnit acquire(DeploymentUnit unit) {
        return executeWithLock(unit, it -> pool.acquire(it, ignored -> new DisposableDeploymentUnit(
                it,
                deployer.unitPath(it.name(), it.version(), true),
                () -> pool.release(it)
        )));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean computeIfNotAcquired(DeploymentUnit unit, Consumer<DeploymentUnit> consumer) {
        return executeWithLock(unit, it -> {
            if (pool.isAcquired(it)) {
                return false;
            } else {
                consumer.accept(it);
                return true;
            }
        });
    }

    private <O> O executeWithLock(DeploymentUnit unit, Function<DeploymentUnit, O> function) {
        Lock lock = locks.acquire(unit, ignored -> new ReentrantLock());
        lock.lock();
        try {
            return function.apply(unit);
        } finally {
            lock.unlock();
            locks.release(unit);
        }
    }
}
