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

package org.apache.ignite.internal.compute.loader;

import static org.apache.ignite.compute.version.Version.LATEST;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.compute.util.DummyDeploymentUnitAccessor;
import org.apache.ignite.internal.deployunit.DisposableDeploymentUnit;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitNotFoundException;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitUnavailableException;
import org.apache.ignite.internal.rest.api.deployment.DeploymentStatus;
import org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JobContextManagerTest {

    private final Path unitsDir = Path.of(JobClassLoaderFactory.class.getClassLoader().getResource("units").getPath());

    private final DummyDeploymentUnitAccessor unitAccessor = spy(new DummyDeploymentUnitAccessor(unitsDir));

    @Mock
    private JobClassLoaderFactory jobClassLoaderFactory;

    private JobContextManager classLoaderManager;

    @BeforeEach
    void setUp() {
        classLoaderManager = new JobContextManager(unitAccessor, jobClassLoaderFactory);
    }

    @Test
    public void acquireAndReleaseClassLoader() {
        List<DeploymentUnit> units = List.of(
                new DeploymentUnit("unit1", "1.0.0"),
                new DeploymentUnit("unit2", "1.0.0")
        );

        List<DisposableDeploymentUnit> deploymentUnits = units.stream()
                .map(unitAccessor::acquire)
                .collect(Collectors.toList());


        JobClassLoader toBeReturned = new JobClassLoader(deploymentUnits, new URL[0], getClass().getClassLoader());
        doReturn(toBeReturned)
                .when(jobClassLoaderFactory).createClassLoader(deploymentUnits);

        JobContext context = classLoaderManager.acquireClassLoader(units).join();
        assertSame(toBeReturned, context.classLoader());

        verify(jobClassLoaderFactory, times(1)).createClassLoader(deploymentUnits); // verify that class loader was created

        context.close();
    }

    @Test
    public void acquireClassLoaderWithLatestVersionTwice() throws IOException {
        Path unitDir = Files.createTempDirectory(unitsDir, "temp-unit");
        unitDir.toFile().deleteOnExit();

        String unitName = unitDir.getFileName().toString();
        DeploymentUnit version1 = new DeploymentUnit(unitName, "1.0.0");
        Path version1path = unitDir.resolve(version1.version().toString());
        List<DisposableDeploymentUnit> disposableVersion1 = List.of(new DisposableDeploymentUnit(version1, version1path, () -> {}));

        DeploymentUnit version2 = new DeploymentUnit(unitName, "2.0.0");
        Path version2path = unitDir.resolve(version2.version().toString());
        List<DisposableDeploymentUnit> disposableVersion2 = List.of(new DisposableDeploymentUnit(version2, version2path, () -> {}));

        try (JobClassLoader toBeReturned1 = new JobClassLoader(
                disposableVersion1,
                extractUrls(disposableVersion1),
                getClass().getClassLoader());
                JobClassLoader toBeReturned2 = new JobClassLoader(
                        disposableVersion2,
                        extractUrls(disposableVersion2),
                        getClass().getClassLoader())
        ) {

            doReturn(toBeReturned1)
                    .when(jobClassLoaderFactory).createClassLoader(eq(disposableVersion1));

            doReturn(toBeReturned2)
                    .when(jobClassLoaderFactory).createClassLoader(eq(disposableVersion2));

            Files.createDirectories(version1path).toFile().deleteOnExit();

            List<DeploymentUnit> units = List.of(new DeploymentUnit(unitName, LATEST));
            JobContext context1 = classLoaderManager.acquireClassLoader(units).join();

            assertSame(toBeReturned1, context1.classLoader());

            JobContext context2 = classLoaderManager.acquireClassLoader(units).join();
            assertSame(context1.classLoader(), context2.classLoader());

            Files.createDirectories(version2path).toFile().deleteOnExit();

            JobContext context3 = classLoaderManager.acquireClassLoader(units).join();
            assertSame(toBeReturned2, context3.classLoader());
        }
    }

    @Test
    public void throwsExceptionOnOnDemandDeploy() {
        doReturn(CompletableFuture.failedFuture(new IOException("Failed to deploy")))
                .when(unitAccessor).onDemandDeploy(any());

        List<DeploymentUnit> units = List.of(
                new DeploymentUnit("unit1", "1.0.0"),
                new DeploymentUnit("unit2", "1.0.0")
        );

        assertThat(
                classLoaderManager.acquireClassLoader(units),
                willThrowFast(IOException.class, "Failed to deploy")
        );
    }

    @Test
    public void nonExistingUnit() {
        List<DeploymentUnit> units = List.of(
                new DeploymentUnit("non-existing", Version.parseVersion("1.0.0"))
        );

        assertThat(
                classLoaderManager.acquireClassLoader(units),
                CompletableFutureExceptionMatcher.willThrow(DeploymentUnitNotFoundException.class, "Unit non-existing:1.0.0 not found")
        );
    }

    @Test
    public void nonExistingVersion() {
        List<DeploymentUnit> units = List.of(
                new DeploymentUnit("unit1", Version.parseVersion("-1.0.0"))
        );

        assertThat(
                classLoaderManager.acquireClassLoader(units),
                CompletableFutureExceptionMatcher.willThrow(DeploymentUnitNotFoundException.class, "Unit unit1:-1.0.0 not found")
        );
    }

    @Test
    public void nonAvailableUnit() {
        DeploymentUnitUnavailableException toBeThrown = new DeploymentUnitUnavailableException(
                "unit",
                Version.parseVersion("1.0.0"),
                DeploymentStatus.OBSOLETE,
                DeploymentStatus.REMOVING
        );

        doThrow(toBeThrown)
                .when(unitAccessor).onDemandDeploy(any());

        assertThat(
                classLoaderManager.acquireClassLoader(List.of(new DeploymentUnit("unit", "1.0.0"))),
                willThrowFast(DeploymentUnitUnavailableException.class, "Unit unit:1.0.0 is unavailable")
        );
    }

    private static URL[] extractUrls(List<DisposableDeploymentUnit> units) {
        return units.stream()
                .map(DisposableDeploymentUnit::path)
                .map(it -> {
                    try {
                        return it.toUri().toURL();
                    } catch (MalformedURLException e) {
                        throw new RuntimeException(e);
                    }
                })
                .toArray(URL[]::new);
    }
}
