package org.apache.ignite.internal.rest.optimiser;

import io.micronaut.http.annotation.Controller;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.rest.ResourceHolder;
import org.apache.ignite.internal.rest.api.optimise.OptimiseApi;
import org.apache.ignite.internal.rest.api.optimise.RunOptimisationRequest;
import org.apache.ignite.internal.rest.api.optimise.RunBenchmarkRequest;
import org.phillippko.ignite.internal.optimiser.OptimiserManager;

@Controller("/management/v1/optimise")
public class OptimiseController implements OptimiseApi, ResourceHolder {
    private OptimiserManager optimiserManager;

    public OptimiseController(OptimiserManager optimiserManager) {
        this.optimiserManager = optimiserManager;
    }

    @Override
    public CompletableFuture<UUID> runBenchmark(RunBenchmarkRequest runBenchmarkRequest) {
        return optimiserManager.runBenchmark(runBenchmarkRequest.nodeName(), runBenchmarkRequest.benchmarkFilePath());
    }

    @Override
    public CompletableFuture<UUID> optimise(RunOptimisationRequest runOptimisationRequest) {
        return optimiserManager.optimise(runOptimisationRequest.nodeName(), runOptimisationRequest.writeIntensive());
    }

    @Override
    public CompletableFuture<String> optimisationResult(UUID id) {
        return optimiserManager.optimisationResult(id);
    }

    @Override
    public void cleanResources() {
        optimiserManager = null;
    }
}
