package org.apache.ignite.schema.internal;

import org.apache.ignite.configuration.internal.ConfigurationManager;

/**
 * This implementation has only one schema.
 * In the future it has schema per version.
 */
public class SchemaManager {
    private final ConfigurationManager configurationMgr;

    /**
     * @param configurationMgr Configuration manager.
     */
    public SchemaManager(ConfigurationManager configurationMgr) {
        this.configurationMgr = configurationMgr;
    }
}
