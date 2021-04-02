package org.apache.ignite.network.configuration;

import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.Value;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

@ConfigurationRoot(rootName = "network", storage = LocalConfigurationStorage.class)
public class NetworkConfigurationSchema {
    /** */
    @Min(1024)
    @Max(0xFFFF)
    @Value
    public int port;

    @Value
    public String[] networkMembersNames;
//
//    @ConfigValue
//    private DiscoveryConfigurationSchema discovery;
}