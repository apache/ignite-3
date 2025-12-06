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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.deployunit.DisposableDeploymentUnit;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class JobClassLoaderFactoryTest extends BaseIgniteAbstractTest {
    private static final String UNIT_JOB_CLASS_NAME = "org.apache.ignite.internal.compute.UnitJob";

    private static final String JOB1_UTILITY_CLASS_NAME = "org.apache.ignite.internal.compute.Job1Utility";

    private static final String JOB2_UTILITY_CLASS_NAME = "org.apache.ignite.internal.compute.Job2Utility";

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
            Class<?> clazz1 = classLoader1.classLoader().loadClass(UNIT_JOB_CLASS_NAME);
            ComputeJob<Void, Integer> job1 = (ComputeJob<Void, Integer>) clazz1.getDeclaredConstructor().newInstance();
            CompletableFuture<Integer> result1 = job1.executeAsync(null, null);
            assertThat(result1, willBe(1));

            // and classes from the second unit are loaded from the second class loader
            Class<?> clazz2 = classLoader2.classLoader().loadClass(UNIT_JOB_CLASS_NAME);
            ComputeJob<Void, String> job2 = (ComputeJob<Void, String>) clazz2.getDeclaredConstructor().newInstance();
            CompletableFuture<String> result2 = job2.executeAsync(null, null);
            assertThat(result2, willBe("Hello World!"));
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
            Class<?> unitJobClass = classLoader.classLoader().loadClass(UNIT_JOB_CLASS_NAME);
            assertNotNull(unitJobClass);

            // and classes are loaded in the aplhabetical order
            ComputeJob<Void, Integer> job1 = (ComputeJob<Void, Integer>) unitJobClass.getDeclaredConstructor().newInstance();
            CompletableFuture<Integer> result1 = job1.executeAsync(null, null);
            assertThat(result1, willBe(1));

            Class<?> job1UtilityClass = classLoader.classLoader().loadClass(JOB1_UTILITY_CLASS_NAME);
            assertNotNull(job1UtilityClass);

            Class<?> job2UtilityClass = classLoader.classLoader().loadClass(JOB2_UTILITY_CLASS_NAME);
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
            Class<?> unitJobClass = classLoader.classLoader().loadClass(UNIT_JOB_CLASS_NAME);
            assertNotNull(unitJobClass);

            Class<?> job1UtilityClass = classLoader.classLoader().loadClass(JOB1_UTILITY_CLASS_NAME);
            assertNotNull(job1UtilityClass);

            Class<?> job2UtilityClass = classLoader.classLoader().loadClass(JOB2_UTILITY_CLASS_NAME);
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
            Class<?> unitJobClass = classLoader.classLoader().loadClass(UNIT_JOB_CLASS_NAME);
            assertNotNull(unitJobClass);

            Class<?> job1UtilityClass = classLoader.classLoader().loadClass(JOB1_UTILITY_CLASS_NAME);
            assertNotNull(job1UtilityClass);

            Class<?> job2UtilityClass = classLoader.classLoader().loadClass(JOB2_UTILITY_CLASS_NAME);
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
            assertThrows(ClassNotFoundException.class, () -> classLoader.classLoader().loadClass(UNIT_JOB_CLASS_NAME));
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
        try (JobClassLoader jobClassLoader = jobClassLoaderFactory.createClassLoader(units)) {
            ClassLoader classLoader = jobClassLoader.classLoader();
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

        assertThrows(
                IllegalArgumentException.class,
                () -> jobClassLoaderFactory.createClassLoader(toDisposableDeploymentUnits(unit)).classLoader()
        );
    }

    @Test
    @DisplayName("Create class loader with non-existing version")
    public void nonExistingVersion() {
        DeploymentUnit unit = new DeploymentUnit("unit1", "1.2.3");

        assertThrows(
                IllegalArgumentException.class,
                () -> jobClassLoaderFactory.createClassLoader(toDisposableDeploymentUnits(unit)).classLoader()
        );
    }

    @Test
    void concurrentAccess() throws InterruptedException {
        List<DisposableDeploymentUnit> units = toDisposableDeploymentUnits(new DeploymentUnit("unit1", "1.0.0"));

        AtomicInteger streamAccessed = new AtomicInteger();

        // Wrap the units list so that the stream() method call takes significant amount of time.
        List<DisposableDeploymentUnit> slowList = new AbstractList<>() {
            @Override
            public int size() {
                return units.size();
            }

            @Override
            public DisposableDeploymentUnit get(int index) {
                return units.get(index);
            }

            @Override
            public Stream<DisposableDeploymentUnit> stream() {
                // The classLoader method in the JobClassLoader should call this once.
                streamAccessed.getAndIncrement();

                // Slow this down to increase a change of multiple threads accessing it concurrently.
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                return super.stream();
            }
        };

        try (JobClassLoader jobClassLoader = jobClassLoaderFactory.createClassLoader(slowList)) {
            List<ClassLoader> classLoaders = new ArrayList<>();

            List<Thread> threads = IntStream.range(0, 10)
                    .mapToObj(i -> new Thread(() -> classLoaders.add(jobClassLoader.classLoader())))
                    .collect(Collectors.toList());

            threads.forEach(Thread::start);

            for (Thread thread : threads) {
                thread.join();
            }

            assertThat(classLoaders, everyItem(sameInstance(classLoaders.get(0))));
            assertThat(streamAccessed.get(), is(1));
        }
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
