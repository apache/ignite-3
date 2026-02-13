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

import static io.micronaut.http.HttpStatus.BAD_REQUEST;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.rest.constants.HttpCode.OK;
import static org.apache.ignite.internal.rest.matcher.MicronautHttpResponseMatcher.assertThrowsProblem;
import static org.apache.ignite.internal.rest.matcher.ProblemMatcher.isProblem;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.disaster.system.SystemDisasterRecoveryManager;
import org.apache.ignite.internal.disaster.system.exception.ClusterResetException;
import org.apache.ignite.internal.disaster.system.exception.MigrateException;
import org.apache.ignite.internal.rest.api.recovery.system.MigrateRequest;
import org.apache.ignite.internal.rest.api.recovery.system.ResetClusterRequest;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@MicronautTest
@Property(name = "ignite.endpoints.filter-non-initialized", value = "false")
@Property(name = "ignite.endpoints.rest-events", value = "false")
@Property(name = "micronaut.security.enabled", value = "false")
class SystemDisasterRecoveryControllerTest extends BaseIgniteAbstractTest {
    @Inject
    @Client("/management/v1/recovery/cluster/")
    private HttpClient client;

    private static final SystemDisasterRecoveryManager systemDisasterRecoveryManager = mock(SystemDisasterRecoveryManager.class);

    @BeforeEach
    void resetMocks() {
        Mockito.reset(systemDisasterRecoveryManager);
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
                new ResetClusterRequest(List.of("a", "b", "c"), null)
        );

        HttpResponse<Void> response = client.toBlocking().exchange(post);

        assertThat(response.getStatus().getCode(), is(OK.code()));
        verify(systemDisasterRecoveryManager).resetCluster(List.of("a", "b", "c"));
    }

    @Test
    void initiatesCmgRepairRepairingMetastorageWithCmgNodeNamesSpecified() {
        int replicationFactor = 1;
        List<String> cmgNodeNames = List.of("a", "b", "c");

        when(systemDisasterRecoveryManager.resetClusterRepairingMetastorage(cmgNodeNames, replicationFactor))
                .thenReturn(nullCompletedFuture());

        HttpRequest<ResetClusterRequest> post = HttpRequest.POST("/reset",
                new ResetClusterRequest(cmgNodeNames, replicationFactor)
        );

        HttpResponse<Void> response = client.toBlocking().exchange(post);

        assertThat(response.getStatus().getCode(), is(OK.code()));
        verify(systemDisasterRecoveryManager).resetClusterRepairingMetastorage(cmgNodeNames, replicationFactor);
    }

    @Test
    void initiatesCmgRepairRepairingMetastorageWithCmgNodeNamesNotSpecified() {
        int replicationFactor = 1;

        when(systemDisasterRecoveryManager.resetClusterRepairingMetastorage(null, replicationFactor))
                .thenReturn(nullCompletedFuture());

        HttpRequest<ResetClusterRequest> post = HttpRequest.POST("/reset",
                new ResetClusterRequest(null, replicationFactor)
        );

        HttpResponse<Void> response = client.toBlocking().exchange(post);

        assertThat(response.getStatus().getCode(), is(OK.code()));
        verify(systemDisasterRecoveryManager).resetClusterRepairingMetastorage(null, replicationFactor);
    }

    @Test
    void resetClusterPassesClusterResetExceptionToClient() {
        when(systemDisasterRecoveryManager.resetCluster(any())).thenReturn(failedFuture(new ClusterResetException("Oops")));

        HttpRequest<ResetClusterRequest> post = HttpRequest.POST("/reset",
                new ResetClusterRequest(List.of("a", "b", "c"), null)
        );

        assertThrowsProblem(
                () -> client.toBlocking().exchange(post),
                isProblem().withStatus(BAD_REQUEST).withDetail("Oops")
        );
    }

    @Test
    void initiatesMigration() {
        ArgumentCaptor<ClusterState> stateCaptor = ArgumentCaptor.forClass(ClusterState.class);

        when(systemDisasterRecoveryManager.migrate(any())).thenReturn(nullCompletedFuture());

        HttpRequest<MigrateRequest> post = HttpRequest.POST("/migrate", migrateRequest());

        HttpResponse<Void> response = client.toBlocking().exchange(post);

        assertThat(response.getStatus().getCode(), is(OK.code()));

        verify(systemDisasterRecoveryManager).migrate(stateCaptor.capture());
        ClusterState capturedState = stateCaptor.getValue();

        assertThat(capturedState, is(notNullValue()));
        assertThat(capturedState.cmgNodes(), containsInAnyOrder("a", "b", "c"));
        assertThat(capturedState.metaStorageNodes(), contains("d"));
        assertThat(capturedState.version(), is("3.0.0"));
        assertThat(capturedState.clusterTag().clusterId(), is(new UUID(1, 2)));
        assertThat(capturedState.clusterTag().clusterName(), is("cluster"));
        assertThat(capturedState.formerClusterIds(), is(List.of(new UUID(2, 1))));
    }

    private static MigrateRequest migrateRequest() {
        return new MigrateRequest(
                List.of("a", "b", "c"),
                List.of("d"),
                "3.0.0",
                new UUID(1, 2),
                "cluster",
                List.of(new UUID(2, 1))
        );
    }

    @Test
    void migratePassesMigrateExceptionToClient() {
        when(systemDisasterRecoveryManager.migrate(any())).thenReturn(failedFuture(new MigrateException("Oops")));

        HttpRequest<MigrateRequest> post = HttpRequest.POST("/migrate", migrateRequest());

        assertThrowsProblem(
                () -> client.toBlocking().exchange(post),
                isProblem().withStatus(BAD_REQUEST).withDetail("Oops")
        );
    }
}
