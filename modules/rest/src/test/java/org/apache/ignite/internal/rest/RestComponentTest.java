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

package org.apache.ignite.internal.rest;

import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.http.client.netty.DefaultHttpClient;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.rest.authentication.AuthenticationProviderFactory;
import org.apache.ignite.internal.rest.cluster.ClusterManagementRestFactory;
import org.apache.ignite.internal.rest.configuration.PresentationsFactory;
import org.apache.ignite.internal.rest.configuration.RestConfiguration;
import org.apache.ignite.internal.security.authentication.AuthenticationManager;
import org.apache.ignite.internal.security.authentication.AuthenticationManagerImpl;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class RestComponentTest extends BaseIgniteAbstractTest {
    private final RestManager restManager = new RestManager();

    private RestComponent restComponent;

    private ClusterState state;

    private HttpClient client;

    @BeforeEach
    public void setup() {

        ConfigurationTreeGenerator generator = new ConfigurationTreeGenerator(RestConfiguration.KEY, NetworkConfiguration.KEY);
        ConfigurationManager configurationManager = new ConfigurationManager(
                List.of(RestConfiguration.KEY, NetworkConfiguration.KEY),
                new TestConfigurationStorage(LOCAL),
                generator,
                new TestConfigurationValidator()
        );
        configurationManager.start();

        RestConfiguration restConfiguration = configurationManager.configurationRegistry().getConfiguration(RestConfiguration.KEY);

        ClusterManagementGroupManager cmg = Mockito.mock(ClusterManagementGroupManager.class);

        Mockito.when(cmg.clusterState()).then(invocation -> CompletableFuture.completedFuture(state));

        AuthenticationManager authenticationManager = new AuthenticationManagerImpl();
        Supplier<RestFactory> authProviderFactory = () -> new AuthenticationProviderFactory(authenticationManager);
        Supplier<RestFactory> restPresentationFactory = () -> new PresentationsFactory(configurationManager, Mockito.mock(ConfigurationManager.class));
        Supplier<RestFactory> clusterManagementRestFactory = () -> new ClusterManagementRestFactory(null, null, cmg);

        restComponent = new RestComponent(
                List.of(restPresentationFactory,
                        authProviderFactory,
                        clusterManagementRestFactory),
                new RestManagerProvider(restManager),
                restConfiguration
        );

        restComponent.start();

        client = new DefaultHttpClient(URI.create("http://localhost:" + restConfiguration.port().value() + "/management/v1/"));
    }

    @AfterEach
    public void cleanup() throws Exception {
        restComponent.stop();
    }

    @Test
    public void nodeConfigTest() {
        var response = client.toBlocking().exchange("configuration/node/", String.class);

        assertEquals(HttpStatus.OK, response.status());
    }

    @Test
    public void nodeConfigDisabledTest() {
        var response = client.toBlocking().exchange("configuration/node/", String.class);

        assertEquals(HttpStatus.OK, response.status());

        restManager.enabled(false);

        HttpClientResponseException e = assertThrows(HttpClientResponseException.class,
                () -> client.toBlocking().exchange("configuration/node/", String.class));

        assertEquals(HttpStatus.NOT_ACCEPTABLE, e.getStatus());

        restManager.enabled(true);

        response = client.toBlocking().exchange("configuration/node/", String.class);

        assertEquals(HttpStatus.OK, response.status());
    }

    @Test
    public void nodeConfigDisabledNotExistTest() {
        var response = client.toBlocking().exchange("configuration/node/", String.class);

        assertEquals(HttpStatus.OK, response.status());

        restManager.enabled(false);

        HttpClientResponseException e = assertThrows(HttpClientResponseException.class,
                () -> client.toBlocking().exchange("configuration/node/", String.class));

        assertEquals(HttpStatus.NOT_ACCEPTABLE, e.getStatus());

        state = Mockito.mock(ClusterState.class);

        HttpClientResponseException e1 = assertThrows(HttpClientResponseException.class,
                () -> client.toBlocking().exchange("configuration/node/", String.class));

        assertEquals(HttpStatus.NOT_ACCEPTABLE, e1.getStatus());
    }
}
