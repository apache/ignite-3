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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.getPath;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.internal.deployunit.DisposableDeploymentUnit;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JobClassLoaderFactoryTest extends BaseIgniteAbstractTest {
    private final Path unitsDir = getPath(JobClassLoaderFactory.class.getClassLoader().getResource("units"));

    private final JobClassLoaderFactory jobClassLoaderFactory = new JobClassLoaderFactory();

    @Test
    @DisplayName("Load class with the same name from two different class loaders")
    public void unit1() throws Exception {

        // when unit1-1.0.0 is loaded and unit2-1.0.0 is loaded
        List<DisposableDeploymentUnit> units1 = toDisposableDeploymentUnits(new DeploymentUnit("unit1", "1.0.0"));
        List<DisposableDeploymentUnit> units2 = toDisposableDeploymentUnits(new DeploymentUnit("unit2", "2.0.0"));

        try (JobClassLoader classLoader1 = jobClassLoaderFactory.createClassLoader(units1);
                JobClassLoader classLoader2 = jobClassLoaderFactory.createClassLoader(units2)) {
            // then classes from the first unit are loaded from the first class loader
            Class<?> clazz1 = classLoader1.loadClass("org.my.job.compute.unit.UnitJob");
            ComputeJob<Integer> job1 = (ComputeJob<Integer>) clazz1.getDeclaredConstructor().newInstance();
            Integer result1 = job1.execute(null);
            assertEquals(1, result1);

            // and classes from the second unit are loaded from the second class loader
            Class<?> clazz2 = classLoader2.loadClass("org.my.job.compute.unit.UnitJob");
            ComputeJob<String> job2 = (ComputeJob<String>) clazz2.getDeclaredConstructor().newInstance();
            String result2 = job2.execute(null);
            assertEquals("Hello World!", result2);
        }
    }

    @Test
    @DisplayName("Load both versions of the unit")
    public void unit1BothVersions() throws Exception {

        List<DisposableDeploymentUnit> units = toDisposableDeploymentUnits(
                new DeploymentUnit("unit1", "1.0.0"),
                new DeploymentUnit("unit1", "2.0.0")
        );

        try (JobClassLoader classLoader = jobClassLoaderFactory.createClassLoader(units)) {
            Class<?> unitJobClass = classLoader.loadClass("org.my.job.compute.unit.UnitJob");
            assertNotNull(unitJobClass);

            // and classes are loaded in the aplhabetical order
            ComputeJob<Integer> job1 = (ComputeJob<Integer>) unitJobClass.getDeclaredConstructor().newInstance();
            Integer result1 = job1.execute(null);
            assertEquals(1, result1);

            Class<?> job1UtilityClass = classLoader.loadClass("org.my.job.compute.unit.Job1Utility");
            assertNotNull(job1UtilityClass);

            Class<?> job2UtilityClass = classLoader.loadClass("org.my.job.compute.unit.Job2Utility");
            assertNotNull(job2UtilityClass);

            // classes from the different units are loaded from the same class loader
            assertSame(job1UtilityClass.getClassLoader(), job2UtilityClass.getClassLoader());
        }
    }

    @Test
    @DisplayName("Unit with multiple jars")
    public void unit1_3_0_1() throws Exception {

        // when unit with multiple jars is loaded
        List<DisposableDeploymentUnit> units = toDisposableDeploymentUnits(new DeploymentUnit("unit1", "3.0.1"));

        // then class from all jars are loaded
        try (JobClassLoader classLoader = jobClassLoaderFactory.createClassLoader(units)) {
            Class<?> unitJobClass = classLoader.loadClass("org.my.job.compute.unit.UnitJob");
            assertNotNull(unitJobClass);

            Class<?> job1UtilityClass = classLoader.loadClass("org.my.job.compute.unit.Job1Utility");
            assertNotNull(job1UtilityClass);

            Class<?> job2UtilityClass = classLoader.loadClass("org.my.job.compute.unit.Job2Utility");
            assertNotNull(job2UtilityClass);
        }
    }

    @Test
    @DisplayName("Unit with multiple jars")
    public void unit1_3_0_2() throws Exception {

        // when unit with multiple jars is loaded
        List<DisposableDeploymentUnit> units = toDisposableDeploymentUnits(new DeploymentUnit("unit1", "3.0.2"));

        // then class from all jars are loaded
        try (JobClassLoader classLoader = jobClassLoaderFactory.createClassLoader(units)) {
            Class<?> unitJobClass = classLoader.loadClass("org.my.job.compute.unit.UnitJob");
            assertNotNull(unitJobClass);

            Class<?> job1UtilityClass = classLoader.loadClass("org.my.job.compute.unit.Job1Utility");
            assertNotNull(job1UtilityClass);

            Class<?> job2UtilityClass = classLoader.loadClass("org.my.job.compute.unit.Job2Utility");
            assertNotNull(job2UtilityClass);
        }
    }

    @Test
    @DisplayName("Corrupted unit")
    public void unit1_4_0_0() {

        // when unit with corrupted jar is loaded
        List<DisposableDeploymentUnit> units = toDisposableDeploymentUnits(new DeploymentUnit("unit1", "4.0.0"));

        // then class loader throws an exception
        try (JobClassLoader classLoader = jobClassLoaderFactory.createClassLoader(units)) {
            assertThrows(ClassNotFoundException.class, () -> classLoader.loadClass("org.my.job.compute.unit.UnitJob"));
        }
    }

    @Test
    @DisplayName("Load resource from unit directory")
    public void unit1_5_0_0() throws IOException {

        Path resourcePath = unitsDir.resolve("unit1/5.0.0/test.txt");
        String expectedContent = Files.readString(resourcePath);

        // when unit with files is loaded
        List<DisposableDeploymentUnit> units = toDisposableDeploymentUnits(new DeploymentUnit("unit1", "5.0.0"));

        // then the files are accessible
        try (JobClassLoader classLoader = jobClassLoaderFactory.createClassLoader(units)) {
            String resource = Files.readString(getPath(classLoader.getResource("test.txt")));
            String subDirResource = Files.readString(getPath(classLoader.getResource("subdir/test.txt")));
            assertEquals(expectedContent, resource);
            assertEquals(expectedContent, subDirResource);

            try (InputStream resourceAsStream = classLoader.getResourceAsStream("test.txt");
                    InputStream subDirResourceAsStream = classLoader.getResourceAsStream("subdir/test.txt")) {
                String resourceStreamString = new String(resourceAsStream.readAllBytes());
                String subDirResourceStreamString = new String(subDirResourceAsStream.readAllBytes());

                assertEquals(expectedContent, resourceStreamString);
                assertEquals(expectedContent, subDirResourceStreamString);
            }
        }
    }

    @Test
    @DisplayName("Create class loader with non-existing unit")
    public void nonExistingUnit() {
        DeploymentUnit unit = new DeploymentUnit("non-existing", "1.0.0");
        DisposableDeploymentUnit disposableDeploymentUnit = new DisposableDeploymentUnit(
                unit,
                unitsDir.resolve(unit.name()).resolve(unit.version().toString()),
                () -> {
                }
        );

        assertThrows(IllegalArgumentException.class, () -> jobClassLoaderFactory.createClassLoader(List.of(disposableDeploymentUnit)));
    }

    @Test
    @DisplayName("Create class loader with non-existing version")
    public void nonExistingVersion() {
        DeploymentUnit unit = new DeploymentUnit("unit1", "-1.0.0");
        DisposableDeploymentUnit disposableDeploymentUnit = new DisposableDeploymentUnit(
                unit,
                unitsDir.resolve(unit.name()).resolve(unit.version().toString()),
                () -> {
                }
        );

        assertThrows(IllegalArgumentException.class, () -> jobClassLoaderFactory.createClassLoader(List.of(disposableDeploymentUnit)));
    }

    private List<DisposableDeploymentUnit> toDisposableDeploymentUnits(DeploymentUnit... units) {
        return Arrays.stream(units)
                .map(it -> new DisposableDeploymentUnit(
                        it,
                        unitsDir.resolve(it.name()).resolve(it.version().toString()),
                        () -> {
                        }
                ))
                .collect(Collectors.toList());
    }
}
