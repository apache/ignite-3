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

import static java.util.function.Predicate.isEqual;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.compute.DeploymentUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DeploymentUnitUndeployerTest {
    private final Set<DeploymentUnit> removingUnits = new CopyOnWriteArraySet<>();

    @Mock
    private DeploymentUnitAccessor deploymentUnitAccessor;


    private DeploymentUnitUndeployer undeployer;

    @BeforeEach
    void setUp() {
        undeployer = new DeploymentUnitUndeployer(
                "testNode",
                deploymentUnitAccessor,
                removingUnits::add
        );
        undeployer.start(500, TimeUnit.MILLISECONDS);
    }

    @Test
    void undeployNotAcquiredUnits() {
        DeploymentUnit unit1 = new DeploymentUnit("unit1", "1.0.0");
        DeploymentUnit unit2 = new DeploymentUnit("unit2", "1.0.0");
        DeploymentUnit unit3 = new DeploymentUnit("unit3", "1.0.0");

        // all units are released.
        doReturn(false).when(deploymentUnitAccessor).isAcquired(any());

        undeployer.undeploy(unit1);
        undeployer.undeploy(unit2);
        undeployer.undeploy(unit3);

        // check all units are removed.
        await().timeout(5, TimeUnit.SECONDS).until(() -> removingUnits.contains(unit1));
        await().timeout(5, TimeUnit.SECONDS).until(() -> removingUnits.contains(unit2));
        await().timeout(5, TimeUnit.SECONDS).until(() -> removingUnits.contains(unit3));
    }

    @Test
    void undeployAcquiredUnits() {
        DeploymentUnit unit1 = new DeploymentUnit("unit1", "1.0.0");
        DeploymentUnit unit2 = new DeploymentUnit("unit2", "1.0.0");
        DeploymentUnit unit3 = new DeploymentUnit("unit3", "1.0.0");

        // all units are acquired and will not be released in the future.
        doReturn(true).when(deploymentUnitAccessor).isAcquired(any());

        undeployer.undeploy(unit1);
        undeployer.undeploy(unit2);
        undeployer.undeploy(unit3);

        // check all units are still not removed.
        await().during(2, TimeUnit.SECONDS).until(
                () -> removingUnits.contains(unit1) && removingUnits.contains(unit2) && removingUnits.contains(unit3),
                isEqual(false)
        );
    }

    @Test
    void undeployReleasedUnits() {
        DeploymentUnit unit1 = new DeploymentUnit("unit1", "1.0.0");
        DeploymentUnit unit2 = new DeploymentUnit("unit2", "1.0.0");
        DeploymentUnit unit3 = new DeploymentUnit("unit3", "1.0.0");

        // unit1 and unit3 will be released in the future.
        doReturn(true, true, false).when(deploymentUnitAccessor).isAcquired(eq(unit1));
        doReturn(true, true, true).when(deploymentUnitAccessor).isAcquired(eq(unit2));
        doReturn(true, false, false).when(deploymentUnitAccessor).isAcquired(eq(unit3));

        undeployer.undeploy(unit1);
        undeployer.undeploy(unit2);
        undeployer.undeploy(unit3);

        // check unit1 and unit3 were removed.
        await().timeout(2, TimeUnit.SECONDS).until(() -> removingUnits.contains(unit1));
        await().timeout(2, TimeUnit.SECONDS).until(() -> removingUnits.contains(unit3));

        // check unit2 is still not removed.
        await().during(2, TimeUnit.SECONDS).until(() -> removingUnits.contains(unit2), isEqual(false));
    }

    @AfterEach
    void tearDown() {
        undeployer.stop();
    }
}
