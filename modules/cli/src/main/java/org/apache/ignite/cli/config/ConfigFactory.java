package org.apache.ignite.cli.config;

import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;

/**
 * Factory for {@link Config}.
 */
@Factory
public class ConfigFactory {
    @Singleton
    public Config fileConfig() {
        return new Config();
    }
}
