package org.apache.ignite.internal.schema.builder;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.schema.builder.SchemaBuilder;
import org.apache.ignite.schema.builder.SchemaValueBuilder;

public class ValueColumnCollectionBuilder implements SchemaValueBuilder {
    private final SchemaConfigurationBuilder schemaBuilder;

    private final List<ValueColumnConfigurationBuilder> cols = new ArrayList<>();

    public ValueColumnCollectionBuilder(SchemaConfigurationBuilder schemaBuilder) {
        this.schemaBuilder = schemaBuilder;
    }

    @Override public ValueColumnConfigurationBuilder addColumn(String name) {
        final ValueColumnConfigurationBuilder bld = new ValueColumnConfigurationBuilder(this);

        bld.withName(name);

        return bld;
    }

    @Override public SchemaBuilder done() {
        assert !cols.isEmpty() : "No value columns configured.";

        cols.forEach(schemaBuilder::addColumn);

        return schemaBuilder;
    }

    void addColumn(ValueColumnConfigurationBuilder colBld) {
        cols.add(colBld);
    }
}
