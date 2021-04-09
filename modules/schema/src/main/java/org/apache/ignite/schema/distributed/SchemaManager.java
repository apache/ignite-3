package org.apache.ignite.schema.distributed;

import java.util.UUID;
import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeType;
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

        this.schema = new SchemaDescriptor(1,
            new Column[] {
                new Column("key", NativeType.INTEGER, false)
            },
            new Column[] {
                new Column("value", NativeType.STRING, false)
            }
        );
    }

    /**
     * Gets a current schema for the table specified.
     *
     * @param tableId Table id.
     * @return Schema.
     */
    public SchemaDescriptor schema(UUID tableId) {
        return schema;
    }

    /**
     * Gets a schema for specific version.
     *
     * @param tableId Table id.
     * @param ver Schema version.
     * @return Schema.
     */
    public SchemaDescriptor schema(UUID tableId, long ver) {
        assert ver >= 0;

        assert schema.version() == ver;

        return schema;
    }
}
