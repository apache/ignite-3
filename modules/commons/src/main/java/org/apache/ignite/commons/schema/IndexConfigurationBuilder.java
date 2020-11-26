package org.apache.ignite.commons.schema;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class IndexConfigurationBuilder {
    private final SchemaConfigurationBuilder schemaBuilder;

    private final Map<String, IndexColumnConfigurationBuilder> cols = new HashMap<>();

    protected String name;
    private int inlineSize;

    public IndexConfigurationBuilder(SchemaConfigurationBuilder schemaBuilder) {
        this.schemaBuilder = schemaBuilder;
    }

    public IndexConfigurationBuilder withName(String name) {
        this.name = name;

        return this;
    }

    public IndexConfigurationBuilder inlineSize(int inlineSize) {
        this.inlineSize = inlineSize;

        return this;
    }

    String name() {
        return name;
    }

    public IndexColumnConfigurationBuilder addIndexColumn(String name) {
        return new IndexColumnConfigurationBuilder(this).withName(name);
    }

    void addIndexColumn(IndexColumnConfigurationBuilder bld) {
        if (cols.put(bld.name(), bld) != null)
            throw new IllegalArgumentException("Index with same name already exists: " + bld.name());
    }

    Collection<IndexColumnConfigurationBuilder> columns() {
        return cols.values();
    }

    public SchemaConfigurationBuilder done() {
        schemaBuilder.addIndex(this);

        return schemaBuilder;
    }
}
