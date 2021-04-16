package org.apache.ignite.configuration.schemas.runner;

import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.storage.ConfigurationType;

/**
 * Configuration schema for cluster endpoint subtree.
 */
@ConfigurationRoot(rootName = "cluster", type = ConfigurationType.DISTRIBUTED)
public class ClusterConfigurationSchema {
    /** List of unique names of those cluster nodes that will host distributed metastorage instances. */
    @Value
    String[] metastorageClusterNodeNames;
}
