package org.apache.ignite.internal.rest.optimiser;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.rest.RestFactory;
import phillippko.org.optimiser.OptimiserManager;

/**
 * Factory that defines beans required for the rest module.
 */
@Factory
public class OptimiserFactory implements RestFactory {
    private OptimiserManager optimiserManager;

    public OptimiserFactory(OptimiserManager optimiserManager) {
        this.optimiserManager = optimiserManager;
    }

    @Bean
    @Singleton
    public OptimiserManager optimiserManager() {
        return optimiserManager;
    }

    @Override
    public void cleanResources() {
        optimiserManager = null;
    }
}
