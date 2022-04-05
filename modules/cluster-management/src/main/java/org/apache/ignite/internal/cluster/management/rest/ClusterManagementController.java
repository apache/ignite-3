package org.apache.ignite.internal.cluster.management.rest;

import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.cluster.management.ClusterInitializer;
import org.apache.ignite.internal.cluster.management.rest.exception.InvalidNodesInitClusterException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;

/**
 * Cluster management controller.
 */
@Controller("/management/v1/cluster/init")
@ApiResponse(responseCode = "400", description = "Incorrect body")
@ApiResponse(responseCode = "500", description = "Internal error")
@Tag(name = "clusterManagement")
public class ClusterManagementController {
    private static final IgniteLogger log = IgniteLogger.forClass(ClusterManagementController.class);

    private final ClusterInitializer clusterInitializer;

    /**
     * Cluster management controller constructor.
     *
     * @param clusterInitializer cluster initializer.
     */
    public ClusterManagementController(ClusterInitializer clusterInitializer) {
        this.clusterInitializer = clusterInitializer;
    }

    /**
     * Initializes cluster.
     */
    @Post
    @Operation(operationId = "init")
    @ApiResponse(responseCode = "200", description = "Cluster initialized")
    @ApiResponse(responseCode = "409", description = "Cluster already initialized with another nodes")
    @Consumes(MediaType.APPLICATION_JSON)
    public void init(@Body InitCommand initCommand) throws ExecutionException, InterruptedException {
        if (log.isInfoEnabled()) {
            log.info("Received init command:\n\tMeta Storage nodes: {}\n\tCMG nodes: {}", initCommand.metaStorageNodes(),
                    initCommand.cmgNodes());
        }

        try {
            clusterInitializer.initCluster(initCommand.metaStorageNodes(), initCommand.cmgNodes(), initCommand.clusterName()).get();
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            handleThrowable(cause);
        }
    }

    private void handleThrowable(Throwable cause) {
        if (cause instanceof IgniteInternalException) {
            throw (IgniteInternalException) cause;
        } else if (cause instanceof IllegalArgumentException) {
            throw new InvalidNodesInitClusterException(cause);
        } else {
            throw new IgniteException(cause);
        }
    }
}
