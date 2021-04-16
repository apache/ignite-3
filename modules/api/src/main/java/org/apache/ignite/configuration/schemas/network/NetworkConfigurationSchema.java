package org.apache.ignite.configuration.schemas.network;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.storage.ConfigurationType;

/**
 * Configuration schema for network endpoint subtree.
 */
@ConfigurationRoot(rootName = "network", type = ConfigurationType.LOCAL)
public class NetworkConfigurationSchema {
    /** */
    @Min(1024)
    @Max(0xFFFF)
    @Value
    public int port;

    /** */
    @Value
    public String[] netMembersNames;
}