package org.apache.ignite.internal.cluster.management.rest;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.cluster.management.ClusterInitializer;
import org.apache.ignite.internal.rest.api.RestFactory;
import org.apache.ignite.network.ClusterService;

/**
 * Factory that creates beans that are needed for {@link ClusterManagementController}.
 */
@Factory
public class ClusterManagementRestFactory implements RestFactory {
    private final ClusterService clusterService;

    public ClusterManagementRestFactory(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Bean
    @Singleton
    public ClusterInitializer clusterInitializer() {
        return new ClusterInitializer(clusterService);
    }
}
