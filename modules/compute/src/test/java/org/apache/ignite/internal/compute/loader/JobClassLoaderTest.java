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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.net.URL;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.internal.compute.JobExecutionContextImpl;
import org.apache.ignite.internal.deployunit.DisposableDeploymentUnit;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JobClassLoaderTest extends BaseIgniteAbstractTest {

    @Mock
    private ClassLoader parentClassLoader;

    private static Stream<Arguments> testArguments() {
        return Stream.of(
                Arguments.of("java.lang.String", String.class),
                Arguments.of("javax.lang.String", String.class),
                Arguments.of("org.apache.ignite.internal.compute.JobExecutionContextImpl", JobExecutionContextImpl.class)
        );
    }

    @ParameterizedTest
    @MethodSource("testArguments")
    public void loadsSystemClassesFirst(String className, Class<?> desiredClass) throws Exception {

        doReturn(desiredClass).when(parentClassLoader).loadClass(className);

        try (TestJobClassLoaderImpl jobClassLoader = spy(new TestJobClassLoaderImpl(new URL[0], List.of(), parentClassLoader))) {
            assertSame(desiredClass, jobClassLoader.loadClass(className));
            verify(jobClassLoader, never()).findClass(className);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"org.apache.ignite.compute.unit1.UnitJob", "java.lang.String", "javax.lang.String"})
    public void loadsOwnClassIfSystemAbsent(String className) throws Exception {
        doThrow(ClassNotFoundException.class).when(parentClassLoader).loadClass(className);

        try (TestJobClassLoaderImpl jobClassLoader = spy(new TestJobClassLoaderImpl(new URL[0], List.of(), parentClassLoader))) {
            Class<TestJobClassLoaderImpl> toBeReturned = TestJobClassLoaderImpl.class;
            doReturn(toBeReturned).when(jobClassLoader).findClass(className);

            assertSame(toBeReturned, jobClassLoader.loadClass(className));
            verify(parentClassLoader, times(1)).loadClass(className);
        }
    }

    @Test
    public void unitsReleasedOnClose() {
        DisposableDeploymentUnit unit1 = mock(DisposableDeploymentUnit.class);
        DisposableDeploymentUnit unit2 = mock(DisposableDeploymentUnit.class);

        try (JobClassLoader jobClassLoader = new JobClassLoader(List.of(unit1, unit2), parentClassLoader)) {
            jobClassLoader.close();
            verify(unit1, times(1)).release();
            verify(unit2, times(1)).release();
        }
    }

    @Test
    public void exceptionsCaughtOnClose() {
        DisposableDeploymentUnit unit1 = mock(DisposableDeploymentUnit.class);
        DisposableDeploymentUnit unit2 = mock(DisposableDeploymentUnit.class);
        RuntimeException toBeThrown = new RuntimeException("Expected exception");
        doThrow(toBeThrown).when(unit1).release();

        JobClassLoader jobClassLoader = new JobClassLoader(List.of(unit1, unit2), parentClassLoader);
        IgniteException igniteException = assertThrows(IgniteException.class, jobClassLoader::close);
        assertThat(igniteException.getSuppressed(), arrayContaining(toBeThrown));
    }

    private static class TestJobClassLoaderImpl extends JobClassLoaderImpl {
        TestJobClassLoaderImpl(URL[] urls, List<DisposableDeploymentUnit> units, ClassLoader parent) {
            super(units, urls, parent);
        }

        @Override
        public Class<?> findClass(String name) throws ClassNotFoundException {
            return super.findClass(name);
        }
    }
}
