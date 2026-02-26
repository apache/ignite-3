package org.apache.ignite.internal.eventlog.impl;

import io.micronaut.context.annotation.Factory;
import io.micronaut.core.annotation.Creator;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.IgniteNodeDetails;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.eventlog.api.EventLog;

/**
 * Factory class for creating instances of {@link EventLog}.
 *
 * This factory provides a method to create and configure an implementation of {@link EventLog} based on the provided
 * configuration registry, Ignite node details, and cluster management group manager. The resulting {@link EventLogImpl}
 * is configured to consume event configuration and handle event logging for the specified Ignite node and cluster state.
 */
@Factory
public class EventLogImplFactory {
    /**
     * Creates an instance of {@link EventLog}.
     *
     * @param configurationRegistry the configuration registry used to obtain the {@link EventLogConfiguration}.
     * @param nodeDetails the details of the Ignite node, including node name and related metadata.
     * @param cmgManager the cluster management group manager used to provide cluster state information.
     * @return a new instance of {@link EventLogImpl} configured with the provided parameters.
     */
    @Singleton
    @Inject
    public static EventLog create(
            @Named("distributed") ConfigurationRegistry configurationRegistry,
            IgniteNodeDetails nodeDetails,
            ClusterManagementGroupManager cmgManager
    ) {
        EventLogConfiguration configuration = configurationRegistry.getConfiguration(EventLogExtensionConfiguration.KEY).eventlog();

        return new EventLogImpl(
                configuration,
                () -> clusterState(cmgManager),
                nodeDetails.nodeName()
        );
    }

    private static UUID clusterState(ClusterManagementGroupManager cmgManager) {
        try {
            return cmgManager.clusterState().get(30, TimeUnit.SECONDS).clusterTag().clusterId();
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }

    }
}
