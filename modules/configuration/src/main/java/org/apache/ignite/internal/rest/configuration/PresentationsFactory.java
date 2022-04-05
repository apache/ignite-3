package org.apache.ignite.internal.rest.configuration;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.rest.presentation.ConfigurationPresentation;
import org.apache.ignite.internal.configuration.rest.presentation.hocon.HoconPresentation;
import org.apache.ignite.internal.rest.api.RestFactory;

/**
 * Factory that defines beans that needed for rest module.
 */
@Factory
public class PresentationsFactory implements RestFactory {
    private final ConfigurationPresentation<String> nodeCfgPresentation;
    private final ConfigurationPresentation<String> clusterCfgPresentation;

    public PresentationsFactory(ConfigurationManager nodeCfgMgr, ConfigurationManager clusterCfgMgr) {
        this.nodeCfgPresentation = new HoconPresentation(nodeCfgMgr.configurationRegistry());
        this.clusterCfgPresentation = new HoconPresentation(clusterCfgMgr.configurationRegistry());
    }

    @Bean
    @Singleton
    @Named("clusterCfgPresentation")
    public ConfigurationPresentation<String> clusterCfgPresentation() {
        return clusterCfgPresentation;
    }

    @Bean
    @Singleton
    @Named("nodeCfgPresentation")
    public ConfigurationPresentation<String> nodeCfgPresentation() {
        return nodeCfgPresentation;
    }
}
