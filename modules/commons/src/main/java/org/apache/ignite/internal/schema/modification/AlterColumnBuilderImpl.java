package org.apache.ignite.internal.schema.modification;

import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.modification.AlterColumnBuilder;
import org.apache.ignite.schema.modification.TableModificationBuilder;

class AlterColumnBuilderImpl implements AlterColumnBuilder {

    private final TableModificationBuilderImpl tableBuilder;

    public AlterColumnBuilderImpl(TableModificationBuilderImpl tableBuilder) {
        this.tableBuilder = tableBuilder;
    }

    @Override public AlterColumnBuilder withNewName(String newName) {
        return this;
    }

    @Override public AlterColumnBuilder convertTo(ColumnType newType) {
        return this;
    }

    @Override public AlterColumnBuilder withNewDefault(Object defaultValue) {
        return this;
    }

    @Override public AlterColumnBuilder asNullable() {
        return this;
    }

    @Override public AlterColumnBuilder asNonNullable(Object replacement) {
        return this;
    }

    @Override public TableModificationBuilder done() {
        return tableBuilder;
    }
}
