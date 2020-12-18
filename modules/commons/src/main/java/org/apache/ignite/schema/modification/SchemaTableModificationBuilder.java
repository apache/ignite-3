package org.apache.ignite.schema.modification;

import org.apache.ignite.schema.builder.TableColumnBuilder;

public interface SchemaTableModificationBuilder {
    TableColumnBuilder addColumn(String name);

    TableColumnBuilder alterColumn(String name);

    SchemaTableModificationBuilder dropColumn(String name);

    SchemaModificationBuilder done();
}
