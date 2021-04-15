package org.apache.ignite.schema.distributed;

import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.internal.schema.SchemaDescriptor;

/**
 * This implementation has only one schema.
 * In the future it has schema per version.
 */
public class SchemaManager {
    private final ConfigurationManager configurationMgr;

    /** Schema. */
    private final SchemaDescriptor schema;

    /**
     * @param configurationMgr Configuration manager.
     */
    public SchemaManager(ConfigurationManager configurationMgr) {
        this.configurationMgr = configurationMgr;
    }
}
