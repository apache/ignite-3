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

package org.apache.ignite.internal.rest.recovery.system;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.annotation.Controller;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.disaster.system.SystemDisasterRecoveryManager;
import org.apache.ignite.internal.rest.ResourceHolder;
import org.apache.ignite.internal.rest.api.recovery.system.ResetClusterRequest;
import org.apache.ignite.internal.rest.api.recovery.system.SystemDisasterRecoveryApi;
import org.apache.ignite.internal.rest.exception.handler.ClusterResetExceptionHandler;
import org.apache.ignite.internal.rest.exception.handler.IgniteInternalExceptionHandler;

/**
 * Controller for system groups disaster recovery.
 */
@Controller("/management/v1/recovery/cluster")
@Requires(classes = {IgniteInternalExceptionHandler.class, ClusterResetExceptionHandler.class})
public class SystemDisasterRecoveryController implements SystemDisasterRecoveryApi, ResourceHolder {
    private SystemDisasterRecoveryManager systemDisasterRecoveryManager;

    public SystemDisasterRecoveryController(SystemDisasterRecoveryManager systemDisasterRecoveryManager) {
        this.systemDisasterRecoveryManager = systemDisasterRecoveryManager;
    }

    @Override
    public CompletableFuture<Void> reset(ResetClusterRequest command) {
        return systemDisasterRecoveryManager.resetCluster(command.cmgNodeNames());
    }

    @Override
    public void cleanResources() {
        systemDisasterRecoveryManager = null;
    }
}
