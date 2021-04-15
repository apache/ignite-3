package org.apache.ignite.configuration.schemas.runner;

import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.storage.ConfigurationType;

/**
 * Cluster wide distributed configuration schema.
 */
@ConfigurationRoot(rootName = "cluster", type = ConfigurationType.DISTRIBUTED)
public class ClusterConfigurationSchema {
    /** It is a list of uniq names of those members which will be stored metadata. */
    @Value
    String[] metastorageMembers;
}
