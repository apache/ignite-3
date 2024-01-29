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

package org.apache.ignite.internal.rest.configuration;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.presentation.ConfigurationPresentation;
import org.apache.ignite.internal.configuration.presentation.HoconPresentation;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.rest.RestManager;
import org.apache.ignite.internal.rest.RestManagerFactory;
import org.apache.ignite.internal.security.authentication.AuthenticationManager;
import org.apache.ignite.internal.security.authentication.AuthenticationManagerImpl;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Functional test for {@link ClusterConfigurationController}.
 */
@MicronautTest
@ExtendWith(ConfigurationExtension.class)
@Property(name = "ignite.endpoints.filter-non-initialized", value = "false")
class ClusterConfigurationControllerTest extends ConfigurationControllerBaseTest {
    @InjectConfiguration
    SecurityConfiguration securityConfiguration;

    @Inject
    @Client("/management/v1/configuration/cluster/")
    HttpClient client;

    @Override
    HttpClient client() {
        return client;
    }

    /**
     * Creates test hocon configuration representation.
     */
    @Bean
    @Named("clusterCfgPresentation")
    @Replaces(factory = PresentationsFactory.class)
    public ConfigurationPresentation<String> cfgPresentation(ConfigurationRegistry configurationRegistry) {
        return new HoconPresentation(configurationRegistry);
    }

    @Factory
    @Bean
    @Replaces(RestManagerFactory.class)
    public RestManagerFactory restManagerProvider() {
        return new RestManagerFactory(new RestManager());
    }

    @Bean
    @Factory
    AuthenticationManager authenticationManager() {
        return new AuthenticationManagerImpl(securityConfiguration);
    }
}
