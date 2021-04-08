package org.apache.ignite.configuration.schemas.runner;

import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.schemas.TempConfigurationStorage;

/**
 * Cluster wide distributed configuration schema.
 */
@ConfigurationRoot(rootName = "cluster", storage = TempConfigurationStorage.class)
public class ClusterConfigurationSchema {
    /** It is a list of uniq names of those members which will be stored metadata. */
    @Value
    String[] metastorageMembers;
}
