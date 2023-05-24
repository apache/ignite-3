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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class JobClassLoaderFactoryTest {
    private final Path units = Path.of(JobClassLoaderFactory.class.getClassLoader().getResource("units").getPath());
    private final JobClassLoaderFactory jobClassLoaderFactory = new JobClassLoaderFactory(units);

    @Test
    @DisplayName("Load class with the same name from two different class loaders")
    public void unit1() throws Exception {

        ClassLoader classLoader1 = jobClassLoaderFactory.createClassLoader(List.of("unit1"));

        assertNotNull(classLoader1);

        Class<?> clazz1 = classLoader1.loadClass("org.my.job.compute.unit.UnitJob");
        Callable<Object> job1 = (Callable<Object>) clazz1.getDeclaredConstructor().newInstance();
        Object result1 = job1.call();
        assertSame(Integer.class, result1.getClass());
        assertEquals(1, result1);

        ClassLoader classLoader2 = jobClassLoaderFactory.createClassLoader(List.of("unit2"));

        assertNotNull(classLoader2);

        Class<?> clazz2 = classLoader2.loadClass("org.my.job.compute.unit.UnitJob");
        Callable<Object> job2 = (Callable<Object>) clazz2.getDeclaredConstructor().newInstance();
        Object result2 = job2.call();
        assertSame(String.class, result2.getClass());
        assertEquals("Hello world!", result2);
    }

    @Test
    @DisplayName("Load the class with the wrong name")
    public void wrongClassName() {
        ClassLoader classLoader1 = jobClassLoaderFactory.createClassLoader(List.of("unit1"));

        assertNotNull(classLoader1);

        assertThrows(ClassNotFoundException.class, () -> classLoader1.loadClass("org.my.job.compute.unit.WrongClass"));
    }

    @Test
    @DisplayName("Unit with multiple jars")
    public void unit3() throws Exception {

        ClassLoader classLoader = jobClassLoaderFactory.createClassLoader(List.of("unit3"));

        assertNotNull(classLoader);

        Class<?> unitJobClass = classLoader.loadClass("org.my.job.compute.unit.UnitJob");
        assertNotNull(unitJobClass);

        Class<?> job1UtilityClass = classLoader.loadClass("org.my.job.compute.unit.Job1Utility");
        assertNotNull(job1UtilityClass);

        Class<?> job2UtilityClass = classLoader.loadClass("org.my.job.compute.unit.Job2Utility");
        assertNotNull(job2UtilityClass);
    }

    @Test
    @DisplayName("Load units in the forward order")
    public void unit1unit2() throws Exception {

        ClassLoader classLoader = jobClassLoaderFactory.createClassLoader(List.of("unit1", "unit2"));

        assertNotNull(classLoader);

        Class<?> unitJobClass = classLoader.loadClass("org.my.job.compute.unit.UnitJob");
        Callable<Object> job = (Callable<Object>) unitJobClass.getDeclaredConstructor().newInstance();
        Object result = job.call();
        assertSame(Integer.class, result.getClass());
        assertEquals(1, result);

        Class<?> job1Utility = classLoader.loadClass("org.my.job.compute.unit.Job1Utility");
        assertNotNull(job1Utility);

        Class<?> job2Utility = classLoader.loadClass("org.my.job.compute.unit.Job2Utility");
        assertNotNull(job2Utility);
    }

    @Test
    @DisplayName("Load units in the reverse order")
    public void unit2unit1() throws Exception {

        ClassLoader classLoader = jobClassLoaderFactory.createClassLoader(List.of("unit2", "unit1"));

        assertNotNull(classLoader);

        Class<?> unitJobClass = classLoader.loadClass("org.my.job.compute.unit.UnitJob");
        Callable<Object> job = (Callable<Object>) unitJobClass.getDeclaredConstructor().newInstance();
        Object result = job.call();
        assertSame(String.class, result.getClass());
        assertEquals("Hello world!", result);

        Class<?> job1Utility = classLoader.loadClass("org.my.job.compute.unit.Job1Utility");
        assertNotNull(job1Utility);

        Class<?> job2Utility = classLoader.loadClass("org.my.job.compute.unit.Job2Utility");
        assertNotNull(job2Utility);
    }

    @Test
    @DisplayName("Load units with jars in subdirectories")
    public void unit4() throws Exception {

        ClassLoader classLoader = jobClassLoaderFactory.createClassLoader(List.of("unit4"));

        assertNotNull(classLoader);

        Class<?> clazz = classLoader.loadClass("org.my.job.compute.unit.UnitJob");
        Callable<Object> job = (Callable<Object>) clazz.getDeclaredConstructor().newInstance();
        Object result = job.call();
        assertSame(Integer.class, result.getClass());
        assertEquals(1, result);
    }

    @Test
    @DisplayName("Corrupted unit")
    public void unit5() {

        ClassLoader classLoader = jobClassLoaderFactory.createClassLoader(List.of("unit5"));

        assertNotNull(classLoader);

        assertThrows(ClassNotFoundException.class, () -> classLoader.loadClass("org.my.job.compute.unit.UnitJob"));
    }

    @Test
    @DisplayName("Load resource from unit directory")
    public void unit6() throws IOException {

        String resourcePath = JobClassLoaderFactoryTest.class.getClassLoader()
                .getResource("units/unit6/test.txt")
                .getPath();
        String expectedContent = Files.readString(Path.of(resourcePath));

        ClassLoader classLoader = jobClassLoaderFactory.createClassLoader(List.of("unit6"));

        assertNotNull(classLoader);

        String resource = Files.readString(Path.of(classLoader.getResource("test.txt").getPath()));
        String resourceAsStream = new String(classLoader.getResourceAsStream("test.txt").readAllBytes());

        assertEquals(expectedContent, resource);
        assertEquals(expectedContent, resourceAsStream);
    }

    @Test
    @DisplayName("Load resource from unit subdirectory")
    public void unit7() throws IOException {

        String resourcePath = JobClassLoaderFactoryTest.class.getClassLoader()
                .getResource("units/unit7/subdir/test.txt")
                .getPath();
        String expectedContent = Files.readString(Path.of(resourcePath));

        ClassLoader classLoader = jobClassLoaderFactory.createClassLoader(List.of("unit7"));

        assertNotNull(classLoader);

        String resource = Files.readString(Path.of(classLoader.getResource("subdir/test.txt").getPath()));
        String resourceAsStream = new String(classLoader.getResourceAsStream("subdir/test.txt").readAllBytes());

        assertEquals(expectedContent, resource);
        assertEquals(expectedContent, resourceAsStream);
    }

    @Test
    @DisplayName("Create class loader with empty units")
    public void emptyUnits() {
        assertThrows(IllegalArgumentException.class, () -> jobClassLoaderFactory.createClassLoader(List.of()));
    }

    @Test
    @DisplayName("Create class loader with non-existing unit")
    public void nonExistingUnit() {
        assertThrows(IllegalArgumentException.class, () -> jobClassLoaderFactory.createClassLoader(List.of("non-existing-unit")));
    }
}
