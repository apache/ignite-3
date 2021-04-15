package org.apache.ignite.configuration.schemas.network;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.schemas.TempConfigurationStorage;

@ConfigurationRoot(rootName = "network", storage = TempConfigurationStorage.class)
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