package org.apache.ignite.internal.schema.builder;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.schema.builder.TableColumnCollectionBuilder;

public class ColumnCollectionBuilder implements TableColumnCollectionBuilder {
    private final SchemaTableBuilderImpl schemaBuilder;

    private final List<TableColumnBuilderImpl> cols = new ArrayList<>();

    public ColumnCollectionBuilder(SchemaTableBuilderImpl schemaBuilder) {
        this.schemaBuilder = schemaBuilder;
    }

    @Override public TableColumnBuilderImpl addColumn(String columnName) {
        final TableColumnBuilderImpl bld = new TableColumnBuilderImpl(this);

        bld.withName(columnName);

        return bld;
    }

    @Override public org.apache.ignite.schema.builder.SchemaTableBuilder done() {
        assert !cols.isEmpty() : "No value columns configured.";

        cols.forEach(schemaBuilder::addColumn);

        return schemaBuilder;
    }

    void addColumn(TableColumnBuilderImpl colBld) {
        cols.add(colBld);
    }
}
