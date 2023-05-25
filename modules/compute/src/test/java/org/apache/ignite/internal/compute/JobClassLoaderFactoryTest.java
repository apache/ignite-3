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

package org.apache.ignite.internal.compute;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
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
import java.util.concurrent.Callable;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.version.Version;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class JobClassLoaderFactoryTest {
    private final Path units = Path.of(JobClassLoaderFactory.class.getClassLoader().getResource("units").getPath());
    private final JobClassLoaderFactory jobClassLoaderFactory = new JobClassLoaderFactory(units);

    @Test
    @DisplayName("Load class with the same name from two different class loaders")
    public void unit1() throws Exception {

        // when unit1-1.0.0 is loaded  and unit2-1.0.0 is loaded
        List<DeploymentUnit> units1 = List.of(new DeploymentUnit("unit1", Version.parse("1.0.0")));
        List<DeploymentUnit> units2 = List.of(new DeploymentUnit("unit2", Version.parse("2.0.0")));

        try (JobClassLoader classLoader1 = jobClassLoaderFactory.createClassLoader(units1);
                JobClassLoader classLoader2 = jobClassLoaderFactory.createClassLoader(units2)) {
            // then classes from the first unit are loaded from the first class loader
            Class<?> clazz1 = classLoader1.loadClass("org.my.job.compute.unit.UnitJob");
            Callable<Object> job1 = (Callable<Object>) clazz1.getDeclaredConstructor().newInstance();
            Object result1 = job1.call();
            assertSame(Integer.class, result1.getClass());
            assertEquals(1, result1);

            // and classes from the second unit are loaded from the second class loader
            Class<?> clazz2 = classLoader2.loadClass("org.my.job.compute.unit.UnitJob");
            Callable<Object> job2 = (Callable<Object>) clazz2.getDeclaredConstructor().newInstance();
            Object result2 = job2.call();
            assertSame(String.class, result2.getClass());
            assertEquals("Hello world!", result2);
        }
    }

    @Test
    @DisplayName("Load unit with the LATEST version")
    public void unit2LatestVersion() throws Exception {
        // when the version is LATEST
        List<DeploymentUnit> units = List.of(new DeploymentUnit("unit2", Version.LATEST));

        try (JobClassLoader classLoader = jobClassLoaderFactory.createClassLoader(units)) {
            // the the unit with the highest version is loaded
            Class<?> clazz = classLoader.loadClass("org.my.job.compute.unit.UnitJob");
            Callable<Object> job = (Callable<Object>) clazz.getDeclaredConstructor().newInstance();
            Object result = job.call();
            assertSame(String.class, result.getClass());
            assertEquals("Hello world!", result);

            // and the unit with the lower version is not loaded
            assertThrows(ClassNotFoundException.class, () -> classLoader.loadClass("org.my.job.compute.unit.Job1Utility"));
        }
    }

    @Test
    @DisplayName("Load both versions of the unit")
    public void unit1BothVersions() throws Exception {
        List<DeploymentUnit> units = List.of(
                new DeploymentUnit("unit1", Version.parse("1.0.0")),
                new DeploymentUnit("unit1", Version.parse("2.0.0"))
        );

        try (JobClassLoader classLoader = jobClassLoaderFactory.createClassLoader(units)) {
            Class<?> unitJobClass = classLoader.loadClass("org.my.job.compute.unit.UnitJob");
            assertNotNull(unitJobClass);

            // and classes are loaded in the aplhabetical order
            Callable<Object> job1 = (Callable<Object>) unitJobClass.getDeclaredConstructor().newInstance();
            Object result1 = job1.call();
            assertSame(Integer.class, result1.getClass());
            assertEquals(1, result1);

            Class<?> job1UtilityClass = classLoader.loadClass("org.my.job.compute.unit.Job1Utility");
            assertNotNull(job1UtilityClass);

            Class<?> job2UtilityClass = classLoader.loadClass("org.my.job.compute.unit.Job2Utility");
            assertNotNull(job2UtilityClass);
        }
    }

    @Test
    @DisplayName("Unit with multiple jars")
    public void unit1_3_0_1() throws Exception {

        // when unit with multiple jars is loaded
        List<DeploymentUnit> units = List.of(
                new DeploymentUnit("unit1", Version.parse("3.0.1"))
        );

        // then class from all jars are loaded
        try (JobClassLoader classLoader = jobClassLoaderFactory.createClassLoader(units)) {

            System.out.println("Classpath: " + Arrays.toString(classLoader.getURLs()));

            Class<?> unitJobClass = classLoader.loadClass("org.my.job.compute.unit.UnitJob");
            assertNotNull(unitJobClass);

            // and classes are loaded in the aplhabetical order
            Callable<Object> job1 = (Callable<Object>) unitJobClass.getDeclaredConstructor().newInstance();
            Object result1 = job1.call();
            assertSame(Integer.class, result1.getClass());
            assertEquals(1, result1);

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
        List<DeploymentUnit> units = List.of(
                new DeploymentUnit("unit1", Version.parse("3.0.2"))
        );

        // then class from all jars are loaded
        try (JobClassLoader classLoader = jobClassLoaderFactory.createClassLoader(units)) {
            Class<?> unitJobClass = classLoader.loadClass("org.my.job.compute.unit.UnitJob");
            assertNotNull(unitJobClass);

            // and classes are loaded in the aplhabetical order
            Callable<Object> job1 = (Callable<Object>) unitJobClass.getDeclaredConstructor().newInstance();
            Object result1 = job1.call();
            assertSame(Integer.class, result1.getClass());
            assertEquals(1, result1);

            Class<?> job1UtilityClass = classLoader.loadClass("org.my.job.compute.unit.Job1Utility");
            assertNotNull(job1UtilityClass);

            Class<?> job2UtilityClass = classLoader.loadClass("org.my.job.compute.unit.Job2Utility");
            assertNotNull(job2UtilityClass);
        }
    }

    @Test
    @DisplayName("Corrupted unit")
    public void unit1_4_0_0() throws IOException {

        // when unit with corrupted jar is loaded
        List<DeploymentUnit> units = List.of(
                new DeploymentUnit("unit1", Version.parse("4.0.0"))
        );

        // then class loader throws an exception
        try (JobClassLoader classLoader = jobClassLoaderFactory.createClassLoader(units)) {
            assertThrows(ClassNotFoundException.class, () -> classLoader.loadClass("org.my.job.compute.unit.UnitJob"));
        }
    }

    @Test
    @DisplayName("Load resource from unit directory")
    public void unit1_5_0_0() throws IOException {

        String resourcePath = JobClassLoaderFactoryTest.class.getClassLoader()
                .getResource("units/unit1/5.0.0/test.txt")
                .getPath();
        String expectedContent = Files.readString(Path.of(resourcePath));

        // when unit with files is loaded
        List<DeploymentUnit> units = List.of(
                new DeploymentUnit("unit1", Version.parse("5.0.0"))
        );

        // then the files are accessible
        try (JobClassLoader classLoader = jobClassLoaderFactory.createClassLoader(units)) {
            String resource = Files.readString(Path.of(classLoader.getResource("test.txt").getPath()));
            String subDirResource = Files.readString(Path.of(classLoader.getResource("subdir/test.txt").getPath()));
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
    @DisplayName("Create class loader with empty units")
    public void emptyUnits() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> jobClassLoaderFactory.createClassLoader(List.of())
        );

        assertThat(exception.getMessage(), containsString("At least one unit must be specified"));
    }

    @Test
    @DisplayName("Create class loader with non-existing unit")
    public void nonExistingUnit() {
        List<DeploymentUnit> units = List.of(
                new DeploymentUnit("non-existing", Version.parse("1.0.0"))
        );
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> jobClassLoaderFactory.createClassLoader(units)
        );
        assertThat(exception.getMessage(), containsString("Unit does not exist:"));
    }

    @Test
    @DisplayName("Create class loader with non-existing version")
    public void nonExistingVersion() {
        List<DeploymentUnit> units = List.of(
                new DeploymentUnit("unit1", Version.parse("-1.0.0"))
        );
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> jobClassLoaderFactory.createClassLoader(units)
        );

        assertThat(exception.getMessage(), containsString("Unit does not exist:"));
    }
}
