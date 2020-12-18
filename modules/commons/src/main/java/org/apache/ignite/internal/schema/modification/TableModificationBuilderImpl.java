package org.apache.ignite.internal.schema.modification;

import org.apache.ignite.internal.schema.SchemaTableImpl;
import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.TableIndex;
import org.apache.ignite.schema.modification.AlterColumnBuilder;
import org.apache.ignite.schema.modification.TableModificationBuilder;

public class TableModificationBuilderImpl implements TableModificationBuilder {

    private final SchemaTableImpl table;

    public TableModificationBuilderImpl(SchemaTableImpl table) {
        this.table = table;
    }

    @Override public TableModificationBuilder addColumn(Column column) {
        return this;
    }

    @Override public TableModificationBuilder addKeyColumn(Column column) {
        return this;
    }

    @Override public AlterColumnBuilder alterColumn(String columnName) {
        return new AlterColumnBuilderImpl(this);
    }

    @Override public TableModificationBuilder dropColumn(String columnName) {
        return this;
    }

    @Override public TableModificationBuilder addIndex(TableIndex index) {
        return this;
    }

    @Override public TableModificationBuilder dropIndex(String indexName) {
        return this;
    }

    @Override public void apply() {

    }
}
