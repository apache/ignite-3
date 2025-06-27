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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DeploymentUnitAccessorImplTest extends BaseIgniteAbstractTest {

    @Mock
    private FileDeployerService deployerService;

    @InjectMocks
    private DeploymentUnitAccessorImpl deploymentUnitAccessor;

    @Test
    void computeIfNotAcquired() {
        DeploymentUnit unit = new DeploymentUnit("unit", "1.0.0");
        DisposableDeploymentUnit disposableDeploymentUnit1 = deploymentUnitAccessor.acquire(unit);
        DisposableDeploymentUnit disposableDeploymentUnit2 = deploymentUnitAccessor.acquire(unit);

        assertFalse(deploymentUnitAccessor.computeIfNotAcquired(unit, ignored -> {}));

        disposableDeploymentUnit1.release();
        assertFalse(deploymentUnitAccessor.computeIfNotAcquired(unit, ignored -> {}));

        disposableDeploymentUnit2.release();
        assertTrue(deploymentUnitAccessor.computeIfNotAcquired(unit, ignored -> {}));
    }
}
