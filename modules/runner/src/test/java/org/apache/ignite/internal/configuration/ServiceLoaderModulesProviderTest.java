/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static com.github.npathai.hamcrestopt.OptionalMatchers.isPresent;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ServiceLoaderModulesProvider}.
 */
class ServiceLoaderModulesProviderTest {
    private final ServiceLoaderModulesProvider provider = new ServiceLoaderModulesProvider();

    @Test
    void loadsModulesDefinedAsServicesInMetaInf() {
        Optional<ConfigurationModule> maybeModule = provider.modules()
                .filter(module -> module.getClass().equals(TestConfigurationModule.class))
                .findAny();

        assertThat(maybeModule, isPresent());
    }
}
