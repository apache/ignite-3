package org.apache.ignite.internal.rest.cluster.optimiser;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.rest.constants.HttpCode.OK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
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
import java.util.UUID;
import org.apache.ignite.internal.rest.api.cluster.RunOptimisationRequest;
import org.apache.ignite.internal.rest.optimiser.OptimiserFactory;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import phillippko.org.optimiser.OptimiserManager;

@MicronautTest
@Property(name = "ignite.endpoints.filter-non-initialized", value = "false")
@Property(name = "micronaut.security.enabled", value = "false")
class OptimiserControllerTest extends BaseIgniteAbstractTest {
    private static final String NODE_NAME = "node1";

    @Inject
    @Client("/management/v1/optimise/")
    private HttpClient client;

    private static final OptimiserManager optimiserManager = mock(OptimiserManager.class);

    @BeforeEach
    void resetMocks() {
        Mockito.reset(optimiserManager);
    }

    @Bean
    @Replaces(OptimiserFactory.class)
    OptimiserFactory optimiserFactory() {
        return new OptimiserFactory(optimiserManager);
    }

    @Test
    void runsOptimise() {
        UUID uuid = UUID.randomUUID();
        boolean writeIntensive = true;

        when(optimiserManager.optimise(NODE_NAME, writeIntensive)).thenReturn(completedFuture(uuid));

        HttpRequest<RunOptimisationRequest> post = HttpRequest.POST("/optimise", RunOptimisationRequest.class)
                .body(new RunOptimisationRequest(NODE_NAME, writeIntensive));

        HttpResponse<Void> response = client.toBlocking().exchange(post);

        assertThat(response.getStatus().getCode(), is(OK.code()));
        verify(optimiserManager).optimise(NODE_NAME, writeIntensive);
    }

    @Test
    void runsBenchmark() {
        UUID uuid = UUID.randomUUID();
        String benchmarkFilePath = "benchmarkPath";

        when(optimiserManager.runBenchmark(NODE_NAME, benchmarkFilePath)).thenReturn(completedFuture(uuid));

        HttpRequest<String> post = HttpRequest.POST("/runBenchmark", String.class).body(benchmarkFilePath);

        HttpResponse<Void> response = client.toBlocking().exchange(post);

        assertThat(response.getStatus().getCode(), is(OK.code()));
        verify(optimiserManager).runBenchmark(NODE_NAME, benchmarkFilePath);
    }
}
