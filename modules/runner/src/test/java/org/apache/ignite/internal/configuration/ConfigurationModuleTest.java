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

package org.apache.ignite.internal.configuration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ConfigurationModuleTest extends BaseIgniteAbstractTest {
    @ParameterizedTest
    @MethodSource("classLoaderSource")
    void createdUsingClassLoader(ClassLoader classLoader) {
        List<ConfigurationModule> modules = CompoundModule.loadAllConfigurationModules(classLoader);

        assertThat(CompoundModule.local(modules).schemaExtensions().size(), is(greaterThan(0)));
        assertThat(CompoundModule.distributed(modules).schemaExtensions().size(), is(greaterThan(0)));
    }

    private static Stream<Arguments> classLoaderSource() {
        return Stream.of(
                Arguments.of((ClassLoader) null),
                Arguments.of(ConfigurationModuleTest.class.getClassLoader())
        );
    }
}
