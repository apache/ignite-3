package org.apache.ignite.internal.compute.executor.platform;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.compute.ComputeJobDataHolder;

@SuppressWarnings("InterfaceMayBeAnnotatedFunctional")
public interface PlatformComputeConnection {
    CompletableFuture<ComputeJobDataHolder> executeJobAsync(
            List<String> deploymentUnitPaths, String jobClassName, ComputeJobDataHolder arg);
}
