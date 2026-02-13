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

import static io.micronaut.http.HttpStatus.CONFLICT;
import static io.micronaut.http.HttpStatus.NOT_FOUND;
import static io.micronaut.http.HttpStatus.OK;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.rest.RestState.INITIALIZATION;
import static org.apache.ignite.internal.rest.RestState.INITIALIZED;
import static org.apache.ignite.internal.rest.matcher.MicronautHttpResponseMatcher.assertThrowsProblem;
import static org.apache.ignite.internal.rest.matcher.MicronautHttpResponseMatcher.hasStatus;
import static org.apache.ignite.internal.rest.matcher.ProblemMatcher.isProblem;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.netty.DefaultHttpClient;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.NodeConfiguration;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.eventlog.api.EventLog;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.configuration.MulticastNodeFinderConfigurationSchema;
import org.apache.ignite.internal.network.configuration.NetworkExtensionConfigurationSchema;
import org.apache.ignite.internal.network.configuration.StaticNodeFinderConfigurationSchema;
import org.apache.ignite.internal.rest.authentication.AuthenticationProviderFactory;
import org.apache.ignite.internal.rest.configuration.PresentationsFactory;
import org.apache.ignite.internal.rest.configuration.RestConfiguration;
import org.apache.ignite.internal.rest.configuration.RestExtensionConfiguration;
import org.apache.ignite.internal.rest.configuration.RestExtensionConfigurationSchema;
import org.apache.ignite.internal.security.authentication.AuthenticationManager;
import org.apache.ignite.internal.security.authentication.AuthenticationManagerImpl;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

/**
 * Test suite for {@link RestComponent}.
 */
public class RestComponentTest extends BaseIgniteAbstractTest {
    private final RestManager restManager = new RestManager();

    private RestComponent restComponent;

    private ClusterState state;

    private HttpClient client;

    @InjectConfiguration
    private SecurityConfiguration securityConfiguration;

    @BeforeEach
    public void setup() {

        ConfigurationTreeGenerator generator = new ConfigurationTreeGenerator(
                List.of(NodeConfiguration.KEY),
                List.of(RestExtensionConfigurationSchema.class, NetworkExtensionConfigurationSchema.class),
                List.of(StaticNodeFinderConfigurationSchema.class, MulticastNodeFinderConfigurationSchema.class)
        );
        ConfigurationManager configurationManager = new ConfigurationManager(
                List.of(NodeConfiguration.KEY),
                new TestConfigurationStorage(LOCAL),
                generator,
                new TestConfigurationValidator()
        );
        assertThat(configurationManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        ConfigurationRegistry configurationRegistry = configurationManager.configurationRegistry();
        RestConfiguration restConfiguration = configurationRegistry.getConfiguration(RestExtensionConfiguration.KEY).rest();

        ClusterManagementGroupManager cmg = mock(ClusterManagementGroupManager.class);

        Mockito.when(cmg.clusterState()).then(invocation -> CompletableFuture.completedFuture(state));

        AuthenticationManager authenticationManager = new AuthenticationManagerImpl(securityConfiguration, EventLog.NOOP);
        Supplier<RestFactory> authProviderFactory = () -> new AuthenticationProviderFactory(authenticationManager);
        Supplier<RestFactory> restPresentationFactory = () -> new PresentationsFactory(
                configurationManager,
                mock(ConfigurationManager.class)
        );
        Supplier<RestFactory> restManagerFactory = () -> new RestManagerFactory(restManager);

        restComponent = new RestComponent(
                List.of(restPresentationFactory,
                        authProviderFactory,
                        restManagerFactory),
                restManager,
                restConfiguration
        );

        assertThat(restComponent.startAsync(new ComponentContext()), willCompleteSuccessfully());

        client = new DefaultHttpClient(URI.create("http://localhost:" + restConfiguration.port().value() + "/management/v1/"));
    }

    @AfterEach
    public void cleanup() {
        assertThat(restComponent.stopAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @ParameterizedTest
    @EnumSource(RestState.class)
    public void nonExistentEndpoint(RestState state) {
        restManager.setState(state);

        assertThrowsProblem(
                () -> client.toBlocking().retrieve("nonExistentEndpoint"),
                isProblem()
                        .withStatus(NOT_FOUND)
                        .withTitle("Not Found")
                        .withDetail("Requested resource not found: /management/v1/nonExistentEndpoint")
        );
    }

    @Test
    public void nodeConfigTest() {
        assertThat(getConfig(), hasStatus(OK));
    }

    @Test
    public void nodeConfigDisabledTest() {
        assertThat(getConfig(), hasStatus(OK));

        restManager.setState(INITIALIZATION);

        assertThrowsProblem(this::getConfig, isProblem().withStatus(CONFLICT));

        restManager.setState(INITIALIZED);

        assertThat(getConfig(), hasStatus(OK));
    }

    @Test
    public void nodeConfigDisabledNotExistTest() {
        assertThat(getConfig(), hasStatus(OK));

        restManager.setState(INITIALIZATION);

        assertThrowsProblem(this::getConfig, isProblem().withStatus(CONFLICT));

        state = mock(ClusterState.class);

        assertThrowsProblem(this::getConfig, isProblem().withStatus(CONFLICT));
    }

    private HttpResponse<String> getConfig() {
        return client.toBlocking().exchange("configuration/node/", String.class);
    }
}
