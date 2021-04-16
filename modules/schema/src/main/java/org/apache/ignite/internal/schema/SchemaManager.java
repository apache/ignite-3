package org.apache.ignite.internal.schema;

import org.apache.ignite.configuration.internal.ConfigurationManager;

/**
 * Schema Manager.
 */
public class SchemaManager {
    /** Configuration manager in order to handle and listen schema specific configuration.*/
    private final ConfigurationManager configurationMgr;

    /**
     * Constructor.
     *
     * @param configurationMgr Configuration manager.
     */
    public SchemaManager(ConfigurationManager configurationMgr) {
        this.configurationMgr = configurationMgr;
    }
}
