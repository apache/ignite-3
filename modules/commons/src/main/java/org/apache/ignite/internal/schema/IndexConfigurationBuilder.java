package org.apache.ignite.internal.schema;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.schema.SchemaIndexBuilder;

public class IndexConfigurationBuilder implements SchemaIndexBuilder {
    private final SchemaConfigurationBuilder schemaBuilder;

    private final Map<String, IndexColumnConfigurationBuilder> cols = new HashMap<>();

    protected String name;

    private int inlineSize;

    public IndexConfigurationBuilder(SchemaConfigurationBuilder schemaBuilder) {
        this.schemaBuilder = schemaBuilder;
    }

    @Override public IndexConfigurationBuilder withName(String name) {
        this.name = name;

        return this;
    }

    @Override public IndexConfigurationBuilder inlineSize(int inlineSize) {
        this.inlineSize = inlineSize;

        return this;
    }

    String name() {
        return name;
    }

    @Override public IndexColumnConfigurationBuilder addIndexColumn(String name) {
        return new IndexColumnConfigurationBuilder(this).withName(name);
    }

    void addIndexColumn(IndexColumnConfigurationBuilder bld) {
        if (cols.put(bld.name(), bld) != null)
            throw new IllegalArgumentException("Index with same name already exists: " + bld.name());
    }

    Collection<IndexColumnConfigurationBuilder> columns() {
        return cols.values();
    }

    @Override public SchemaConfigurationBuilder done() {
        schemaBuilder.addIndex(this);

        return schemaBuilder;
    }
}
