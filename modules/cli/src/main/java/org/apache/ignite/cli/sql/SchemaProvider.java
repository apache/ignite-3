package org.apache.ignite.cli.sql;

/**
 * Database schema provider.
 */
public interface SchemaProvider {
    /**
     * Retrieves DB schema.
     *
     * @return instance of {@link SqlSchema}.
     */
    SqlSchema getSchema();
}
