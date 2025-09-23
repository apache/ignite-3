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

package org.apache.ignite.internal.client;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import java.io.File;
import java.util.List;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.cli.call.cluster.unit.DeployUnitClient;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.model.DeployMode;

/**
 * Utils for client compatibility tests.
 */
class ClientCompatibilityTestUtils {
    static DeploymentUnit deployUnit(String apiBasePath, List<File> unitFiles, String unitName, String unitVersion)
            throws Exception {
        // TODO IGNITE-26418 Netty buffer leaks in REST API
        ResourceLeakDetector.setLevel(Level.DISABLED);

        DeployUnitClient deployUnitClient = new DeployUnitClient(new ApiClient().setBasePath(apiBasePath));
        Boolean deployRes = deployUnitClient.deployUnit(unitName, unitFiles, unitVersion, DeployMode.ALL, List.of());

        assertTrue(deployRes);

        return new DeploymentUnit(unitName, unitVersion);
    }
}
