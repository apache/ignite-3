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

package org.apache.ignite.internal.cli.call.cluster.unit;

import java.util.List;
import okhttp3.Call;
import org.apache.ignite.rest.client.model.DeployMode;
import org.jetbrains.annotations.Nullable;

/** Abstraction for deployment content that can be either files or a ZIP archive. */
interface DeploymentContent {
    /**
     * Executes the deployment and returns the HTTP call.
     *
     * @param api The deployment client.
     * @param unitId The unit ID.
     * @param version The unit version.
     * @param deployMode The deployment mode.
     * @param initialNodes The initial nodes to deploy to.
     * @param callback The callback for tracking progress.
     * @return The HTTP call.
     */
    Call deploy(
            DeployUnitClient api,
            String unitId,
            String version,
            @Nullable DeployMode deployMode,
            @Nullable List<String> initialNodes,
            TrackingCallback<Boolean> callback
    );

    /** Performs cleanup after deployment (e.g., deletes temporary files). */
    void cleanup();
}
