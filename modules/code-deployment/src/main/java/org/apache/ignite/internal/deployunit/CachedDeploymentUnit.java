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
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Implementation of {@link DeploymentUnit} that handles unit from provided future.
 */
public class CachedDeploymentUnit implements DeploymentUnit {
    private static final IgniteLogger LOG = Loggers.forClass(CachedDeploymentUnit.class);

    private final CompletableFuture<DeploymentUnit> future;

    public CachedDeploymentUnit(CompletableFuture<DeploymentUnit> future) {
        this.future = future;
    }

    @Override
    public <T, R> CompletableFuture<R> process(DeploymentUnitProcessor<T, R> processor, T arg) {
        return future.thenCompose(unit -> unit.process(processor, arg));
    }

    @Override
    public void close() throws Exception {
        future.whenComplete((unit, throwable) -> {
            if (unit != null) {
                try {
                    unit.close();
                } catch (Exception e) {
                    LOG.warn("Failed to close deployment unit: {}", e, unit);
                }
            } else {
                LOG.info("Future to access cached unit is failed.", throwable);
            }
        });
    }
}
