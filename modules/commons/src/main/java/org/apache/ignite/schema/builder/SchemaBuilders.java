package org.apache.ignite.schema.builder;

import org.apache.ignite.internal.schema.builder.HashIndexBuilderImpl;
import org.apache.ignite.internal.schema.builder.PartialIndexBuilderImpl;
import org.apache.ignite.internal.schema.builder.SchemaTableBuilderImpl;
import org.apache.ignite.internal.schema.builder.SortedIndexBuilderImpl;
import org.apache.ignite.schema.SchemaTable;

/**
 * Schema builder helper.
 */
public final class SchemaBuilders {
    /** Default schema name. */
    public static final String DEFAULT_SCHEMA_NAME = "PUBLIC";

    /**
     * Creates table descriptor builder.
     *
     * @param schemaName Schema name.
     * @param tableName Table name.
     * @return Table descriptor builder.
     */
    public static SchemaTableBuilder tableBuilder(String schemaName,
        String tableName) {
        return new SchemaTableBuilderImpl(schemaName, tableName);
    }

    /**
     * Create table descriptor from classes.
     *
     * @param schemaName Schema name.
     * @param keyClass Key class.
     * @param valueClass Value class.
     * @return Table descriptor for given key-value pair.
     */
    public static SchemaTable buildSchema(String schemaName, String tableName, Class<?> keyClass, Class<?> valueClass) {
        // TODO: implement schema generation from classes.

        return null;
    }

    /**
     * @param name Index name.
     * @return Sorted index builder.
     */
    public static SortedIndexBuilder sorted(String name) {
        return new SortedIndexBuilderImpl(name);
    }

    /**
     * @param name Index name.
     * @return Partial index builder.
     */
    public static PartialIndexBuilder partial(String name) {
        return new PartialIndexBuilderImpl(name);
    }

    /**
     * @param name Index name.
     * @return Hash index builder.
     */
    public static HashIndexBuilder hash(String name) {
        return new HashIndexBuilderImpl(name);
    }

    // Stub.
    private SchemaBuilders() {
    }
}
