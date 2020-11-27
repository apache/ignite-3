package org.apache.ignite.internal.schema;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.schema.SchemaBuilder;
import org.apache.ignite.schema.SchemaKeyBuilder;

public class KeyColumnCollectionBuilder implements SchemaKeyBuilder {
    private final SchemaConfigurationBuilder schemaBuilder;

    private final List<KeyColumnConfigurationBuilder> cols = new ArrayList<>();

    public KeyColumnCollectionBuilder(SchemaConfigurationBuilder schemaBuilder) {
        this.schemaBuilder = schemaBuilder;
    }

    @Override public KeyColumnConfigurationBuilder addColumn(String name) {
        final KeyColumnConfigurationBuilder bld = new KeyColumnConfigurationBuilder(this);

        bld.withName(name);

        return bld;
    }

    @Override public SchemaBuilder done() {
        assert !cols.isEmpty() : "No key columns configured.";

        // TODO: are next mandatory requirements?
        assert cols.stream().anyMatch(c -> !c.isNullable()) : "At least one column must be not null.";
        assert cols.stream().allMatch(c -> !c.isAffinityColumn() || !c.isNullable()) : "Affinity column must be non-null.";

        cols.forEach(schemaBuilder::addColumn);

        return schemaBuilder;
    }

    void addColumn(KeyColumnConfigurationBuilder colBld) {
        cols.add(colBld);
    }
}
