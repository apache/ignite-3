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
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.util.List;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.Test;

class JobClassLoaderFactoryTest {


    private final File units = new File(JobClassLoaderFactory.class.getClassLoader().getResource("units").getPath());
    private final JobClassLoaderFactory jobClassLoaderFactory = new JobClassLoaderFactory(units);

    @Test
    public void unit1() throws Exception {

        ClassLoader classLoader = jobClassLoaderFactory.createClassLoader(List.of("unit1"));

        assertNotNull(classLoader);

        Class<?> clazz = classLoader.loadClass("org.my.job.compute.unit.UnitJob");
        Callable<Object> job = (Callable<Object>) clazz.getDeclaredConstructor().newInstance();
        Object result = job.call();
        assertThat(result.getClass(), is(Integer.class));
        assertEquals(1, result);
    }

    @Test
    public void unit2() throws Exception {

        ClassLoader classLoader = jobClassLoaderFactory.createClassLoader(List.of("unit2"));

        assertNotNull(classLoader);

        Class<?> clazz = classLoader.loadClass("org.my.job.compute.unit.UnitJob");
        Callable<Object> job = (Callable<Object>) clazz.getDeclaredConstructor().newInstance();
        Object result = job.call();
        assertThat(result.getClass(), is(String.class));
        assertEquals("Hello world!", result);
    }


    @Test
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
    public void unit1unit2() throws Exception {

        ClassLoader classLoader = jobClassLoaderFactory.createClassLoader(List.of("unit1", "unit2"));

        assertNotNull(classLoader);

        Class<?> unitJobClass = classLoader.loadClass("org.my.job.compute.unit.UnitJob");
        Callable<Object> job = (Callable<Object>) unitJobClass.getDeclaredConstructor().newInstance();
        Object result = job.call();
        assertThat(result.getClass(), is(Integer.class));
        assertEquals(1, result);

        Class<?> job1Utility = classLoader.loadClass("org.my.job.compute.unit.Job1Utility");
        assertNotNull(job1Utility);

        Class<?> job2Utility = classLoader.loadClass("org.my.job.compute.unit.Job2Utility");
        assertNotNull(job2Utility);
    }

    @Test
    public void unit2unit1() throws Exception {

        ClassLoader classLoader = jobClassLoaderFactory.createClassLoader(List.of("unit2", "unit1"));

        assertNotNull(classLoader);

        Class<?> unitJobClass = classLoader.loadClass("org.my.job.compute.unit.UnitJob");
        Callable<Object> job = (Callable<Object>) unitJobClass.getDeclaredConstructor().newInstance();
        Object result = job.call();
        assertThat(result.getClass(), is(String.class));
        assertEquals("Hello world!", result);

        Class<?> job1Utility = classLoader.loadClass("org.my.job.compute.unit.Job1Utility");
        assertNotNull(job1Utility);

        Class<?> job2Utility = classLoader.loadClass("org.my.job.compute.unit.Job2Utility");
        assertNotNull(job2Utility);
    }
}
