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

import static org.junit.jupiter.api.Assertions.assertSame;

import java.net.URL;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JobClassLoaderTest {

    private static Stream<Arguments> testArguments() {
        return Stream.of(
                Arguments.of("java.lang.String", String.class),
                Arguments.of("javax.lang.String", String.class),
                Arguments.of("org.apache.ignite.internal.compute.JobExecutionContextImpl", JobExecutionContextImpl.class)
        );
    }

    @ParameterizedTest
    @MethodSource("testArguments")
    public void test(String className, Class<?> desiredClass) throws Exception {
        TestParentClassLoader testParentClassLoader = new TestParentClassLoader(desiredClass);

        try (JobClassLoader parentJobClassLoader = new JobClassLoader(new URL[0], testParentClassLoader);
                JobClassLoader jobClassLoader = new JobClassLoader(new URL[0], parentJobClassLoader)) {
            assertSame(desiredClass, jobClassLoader.loadClass(className));
        }
    }

    private static class TestParentClassLoader extends ClassLoader {

        private final Class<?> desiredClass;

        TestParentClassLoader(Class<?> desiredClass) {
            this.desiredClass = desiredClass;
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) {
            return desiredClass;
        }
    }
}
