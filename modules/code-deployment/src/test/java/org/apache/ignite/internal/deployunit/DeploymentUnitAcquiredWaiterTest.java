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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DeploymentUnitAcquiredWaiterTest extends BaseIgniteAbstractTest {
    private static final int DELAY_IN_MILLIS = 500;

    private final Set<DeploymentUnit> removingUnits = new CopyOnWriteArraySet<>();

    @Mock
    private FileDeployerService deployerService;

    @Spy
    @InjectMocks
    private DeploymentUnitAccessorImpl deploymentUnitAccessor;

    private DeploymentUnitAcquiredWaiter undeployer;

    @BeforeEach
    void setUp() {
        undeployer = new DeploymentUnitAcquiredWaiter(
                "testNode",
                deploymentUnitAccessor,
                removingUnits::add
        );
        undeployer.start(DELAY_IN_MILLIS, TimeUnit.MILLISECONDS);
    }

    @Test
    void undeployNotAcquiredUnits() {
        DeploymentUnit unit1 = new DeploymentUnit("unit1", "1.0.0");
        DeploymentUnit unit2 = new DeploymentUnit("unit2", "1.0.0");
        DeploymentUnit unit3 = new DeploymentUnit("unit3", "1.0.0");

        undeployer.submitToAcquireRelease(unit1);
        undeployer.submitToAcquireRelease(unit2);
        undeployer.submitToAcquireRelease(unit3);

        // check all units are removed instantly.
        assertThat(removingUnits, contains(unit1, unit2, unit3));
    }

    @Test
    void undeployAcquiredUnits() {
        DeploymentUnit unit1 = new DeploymentUnit("unit1", "1.0.0");
        DeploymentUnit unit2 = new DeploymentUnit("unit2", "1.0.0");
        DeploymentUnit unit3 = new DeploymentUnit("unit3", "1.0.0");

        // all units are acquired and will not be released in the future.
        deploymentUnitAccessor.acquire(unit1);
        deploymentUnitAccessor.acquire(unit2);
        deploymentUnitAccessor.acquire(unit3);

        undeployer.submitToAcquireRelease(unit1);
        undeployer.submitToAcquireRelease(unit2);
        undeployer.submitToAcquireRelease(unit3);

        // check all units are still not removed.
        await().during(DELAY_IN_MILLIS * 5, TimeUnit.MILLISECONDS).until(
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
        DisposableDeploymentUnit deploymentUnit1 = deploymentUnitAccessor.acquire(unit1);
        DisposableDeploymentUnit deploymentUnit2 = deploymentUnitAccessor.acquire(unit2);
        DisposableDeploymentUnit deploymentUnit3 = deploymentUnitAccessor.acquire(unit3);

        undeployer.submitToAcquireRelease(unit1);
        undeployer.submitToAcquireRelease(unit2);
        undeployer.submitToAcquireRelease(unit3);

        verify(deploymentUnitAccessor, atLeastOnce()).computeIfNotAcquired(eq(unit1), any());
        verify(deploymentUnitAccessor, atLeastOnce()).computeIfNotAcquired(eq(unit2), any());
        verify(deploymentUnitAccessor, atLeastOnce()).computeIfNotAcquired(eq(unit3), any());

        assertThat(removingUnits, emptyIterable());

        deploymentUnit1.release();
        deploymentUnit3.release();

        // check unit1 and unit3 were removed.
        await().timeout(DELAY_IN_MILLIS * 4, TimeUnit.MILLISECONDS).until(() -> removingUnits.contains(unit1));
        await().timeout(DELAY_IN_MILLIS * 4, TimeUnit.MILLISECONDS).until(() -> removingUnits.contains(unit3));

        // check unit2 is still not removed.
        await().during(DELAY_IN_MILLIS * 4, TimeUnit.MILLISECONDS)
                .until(() -> removingUnits.contains(unit2), isEqual(false));

        deploymentUnit2.release();
    }

    @Test
    void notInfinityLoop() {
        DeploymentUnit unit1 = new DeploymentUnit("unit1", "1.0.0");

        deploymentUnitAccessor.acquire(unit1);

        undeployer.submitToAcquireRelease(unit1);

        // check delay between attempts to undeploy the unit.
        verify(deploymentUnitAccessor, after(DELAY_IN_MILLIS * 5).atMost(6))
                .computeIfNotAcquired(eq(unit1), any());
    }

    @AfterEach
    void tearDown() {
        undeployer.stop();
    }
}
