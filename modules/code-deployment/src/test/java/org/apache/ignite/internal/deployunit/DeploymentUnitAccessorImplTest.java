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


import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;

import java.nio.file.Path;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitNotFoundException;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitUnavailableException;
import org.apache.ignite.internal.rest.api.deployment.DeploymentStatus;
import org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher;
import org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DeploymentUnitAccessorImplTest {

    @Mock
    private IgniteDeployment deployment;

    private DeploymentUnitAccessorImpl unitAccessor;

    @BeforeEach
    void setUp() {
        lenient().doReturn(completedFuture(null))
                .when(deployment).onDemandDeploy(anyString(), any());

        unitAccessor = new DeploymentUnitAccessorImpl(deployment);
    }

    @Test
    public void detectLatestDeployedVersion() {
        UnitStatuses statuses = UnitStatuses.builder("unit")
                .append(Version.parseVersion("3.0.0"), DeploymentStatus.DEPLOYED)
                .append(Version.parseVersion("5.0.0"), DeploymentStatus.UPLOADING)
                .append(Version.parseVersion("4.0.0"), DeploymentStatus.DEPLOYED)
                .append(Version.parseVersion("1.0.0"), DeploymentStatus.OBSOLETE)
                .append(Version.parseVersion("2.0.0"), DeploymentStatus.REMOVING)
                .build();

        doReturn(completedFuture(statuses)).when(deployment).clusterStatusesAsync("unit");

        assertThat(
                unitAccessor.detectLatestDeployedVersion("unit"),
                CompletableFutureMatcher.willBe(Version.parseVersion("5.0.0"))
        );
    }

    @Test
    public void detectLatestDeployedVersionWhenNoDeployedVersions() {
        UnitStatuses statuses = UnitStatuses.builder("unit")
                .append(Version.parseVersion("1.0.0"), DeploymentStatus.OBSOLETE)
                .append(Version.parseVersion("2.0.0"), DeploymentStatus.REMOVING)
                .build();

        doReturn(completedFuture(statuses)).when(deployment).clusterStatusesAsync("unit");

        assertThat(
                unitAccessor.detectLatestDeployedVersion("unit"),
                CompletableFutureExceptionMatcher.willThrow(DeploymentUnitNotFoundException.class)
        );
    }

    @Test
    public void detectLatestDeployedVersionOfNonExistingUnit() {
        UnitStatuses statuses = UnitStatuses.builder("unit").build();
        doReturn(completedFuture(statuses)).when(deployment).clusterStatusesAsync("unit");

        assertThat(
                unitAccessor.detectLatestDeployedVersion("unit"),
                CompletableFutureExceptionMatcher.willThrow(DeploymentUnitNotFoundException.class)
        );
    }

    @Test
    public void acquireDeployedUnitToCluster() {
        DeploymentUnit unit = new DeploymentUnit("unit", Version.parseVersion("4.0.0"));
        Path toBeReturned = Path.of("/workDir/unit/4.0.0");
        doReturn(completedFuture(toBeReturned)).when(deployment).path(unit.name(), unit.version());
        doReturn(completedFuture(DeploymentStatus.DEPLOYED)).when(deployment).clusterStatusAsync(unit.name(), unit.version());

        DisposableDeploymentUnit expected = new DisposableDeploymentUnit(unit, toBeReturned, () -> {});
        assertThat(
                unitAccessor.acquire(unit),
                CompletableFutureMatcher.willBe(expected)
        );
    }

    @Test
    public void acquireUploadingUnitToCluster() {
        DeploymentUnit unit = new DeploymentUnit("unit", Version.parseVersion("4.0.0"));
        Path toBeReturned = Path.of("/workDir/unit/4.0.0");
        doReturn(completedFuture(toBeReturned)).when(deployment).path(unit.name(), unit.version());
        doReturn(completedFuture(DeploymentStatus.DEPLOYED)).when(deployment).clusterStatusAsync(unit.name(), unit.version());

        DisposableDeploymentUnit expected = new DisposableDeploymentUnit(unit, toBeReturned, () -> {});
        assertThat(
                unitAccessor.acquire(unit),
                CompletableFutureMatcher.willBe(expected)
        );
    }

    @Test
    public void acquireObsoleteUnit() {
        DeploymentUnit unit = new DeploymentUnit("unit", Version.parseVersion("4.0.0"));
        doReturn(completedFuture(DeploymentStatus.OBSOLETE)).when(deployment).clusterStatusAsync(unit.name(), unit.version());
        doReturn(completedFuture(DeploymentStatus.OBSOLETE)).when(deployment).nodeStatusAsync(unit.name(), unit.version());

        assertThat(
                unitAccessor.acquire(unit),
                CompletableFutureExceptionMatcher.willThrow(DeploymentUnitUnavailableException.class)
        );
    }

    @Test
    public void acquireRemovingUnit() {
        DeploymentUnit unit = new DeploymentUnit("unit", Version.parseVersion("4.0.0"));
        doReturn(completedFuture(DeploymentStatus.REMOVING)).when(deployment).clusterStatusAsync(unit.name(), unit.version());
        doReturn(completedFuture(DeploymentStatus.REMOVING)).when(deployment).nodeStatusAsync(unit.name(), unit.version());

        assertThat(
                unitAccessor.acquire(unit),
                CompletableFutureExceptionMatcher.willThrow(DeploymentUnitUnavailableException.class)
        );
    }
}
