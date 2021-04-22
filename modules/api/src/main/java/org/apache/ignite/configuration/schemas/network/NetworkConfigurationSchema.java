package org.apache.ignite.configuration.schemas.network;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.storage.ConfigurationType;

@SuppressWarnings("PMD.UnusedPrivateField")
@ConfigurationRoot(rootName = "network", type = ConfigurationType.LOCAL)
public class NetworkConfigurationSchema {
    /** Uniq local node name. */
    @Value(hasDefault = true)
    public String name = "";

    /**
     *
     */
    @Min(1024)
    @Max(0xFFFF)
    @Value (hasDefault = true)
    public int port = 47500;

    @Value (hasDefault = true)
    public String[] netClusterNodes = new String[0];
//
//    @ConfigValue
//    private DiscoveryConfigurationSchema discovery;
}