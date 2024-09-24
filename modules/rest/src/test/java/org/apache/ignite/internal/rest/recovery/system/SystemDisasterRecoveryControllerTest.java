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

package org.apache.ignite.internal.rest.recovery.system;

import static com.github.npathai.hamcrestopt.OptionalMatchers.isPresent;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.rest.constants.HttpCode.BAD_REQUEST;
import static org.apache.ignite.internal.rest.constants.HttpCode.OK;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.disaster.system.SystemDisasterRecoveryManager;
import org.apache.ignite.internal.disaster.system.exception.ClusterResetException;
import org.apache.ignite.internal.rest.RestManager;
import org.apache.ignite.internal.rest.RestManagerFactory;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.rest.api.recovery.system.ResetClusterRequest;
import org.apache.ignite.internal.security.authentication.AuthenticationManager;
import org.apache.ignite.internal.security.authentication.AuthenticationManagerImpl;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

@MicronautTest
@ExtendWith(ConfigurationExtension.class)
class SystemDisasterRecoveryControllerTest extends BaseIgniteAbstractTest {
    @InjectConfiguration
    private SecurityConfiguration securityConfiguration;

    @Inject
    @Client("/management/v1/recovery/cluster/")
    private HttpClient client;

    private static final SystemDisasterRecoveryManager systemDisasterRecoveryManager = mock(SystemDisasterRecoveryManager.class);

    @BeforeEach
    void resetMocks() {
        Mockito.reset(systemDisasterRecoveryManager);
    }

    @Factory
    @Bean
    @Replaces(RestManagerFactory.class)
    RestManagerFactory restManagerProvider() {
        return new RestManagerFactory(new RestManager());
    }

    @Bean
    @Factory
    AuthenticationManager authenticationManager() {
        return new AuthenticationManagerImpl(securityConfiguration, ign -> {});
    }

    @Bean
    @Replaces(SystemDisasterRecoveryFactory.class)
    SystemDisasterRecoveryFactory systemDisasterRecoveryFactory() {
        return new SystemDisasterRecoveryFactory(systemDisasterRecoveryManager);
    }

    @Test
    void initiatesCmgRepair() {
        when(systemDisasterRecoveryManager.resetCluster(List.of("a", "b", "c"))).thenReturn(nullCompletedFuture());

        HttpRequest<ResetClusterRequest> post = HttpRequest.POST("/reset",
                new ResetClusterRequest(List.of("a", "b", "c")));

        HttpResponse<Void> response = client.toBlocking().exchange(post);

        assertThat(response.getStatus().getCode(), is(OK.code()));
        verify(systemDisasterRecoveryManager).resetCluster(List.of("a", "b", "c"));
    }

    @Test
    void resetClusterPassesClusterResetExceptionToClient() {
        when(systemDisasterRecoveryManager.resetCluster(any())).thenReturn(failedFuture(new ClusterResetException("Oops")));

        HttpRequest<ResetClusterRequest> post = HttpRequest.POST("/reset",
                new ResetClusterRequest(List.of("a", "b", "c")));

        HttpClientResponseException ex = assertThrows(HttpClientResponseException.class, () -> client.toBlocking().exchange(post));

        assertThat(ex.getStatus().getCode(), is(BAD_REQUEST.code()));

        Optional<Problem> body = ex.getResponse().getBody(Problem.class);
        assertThat(body, isPresent());
        assertThat(body.get().detail(), is("Oops"));
    }
}
