package org.apache.ignite.internal.schema.event;

import java.util.UUID;
import org.apache.ignite.internal.manager.EventParameters;
import org.apache.ignite.internal.schema.SchemaRegistry;

/**
 * Schema event parameters. There are properties which associate with a concrete schema.
 */
public class SchemaEventParameters implements EventParameters {
    /** Table identifier. */
    private final UUID tableId;

    /** Schema registry. */
    private final SchemaRegistry reg;

    /**
     * @param tableId Table identifier.
     * @param reg Schema registry for the table.
     */
    public SchemaEventParameters(UUID tableId, SchemaRegistry reg) {
        this.tableId = tableId;
        this.reg = reg;
    }

    /**
     * Get the table identifier.
     *
     * @return Table id.
     */
    public UUID tableId() {
        return tableId;
    }

    /**
     * Get schema registry for the table.
     *
     * @return Schema registry.
     */
    public SchemaRegistry schemaRegistry() {
        return reg;
    }
}