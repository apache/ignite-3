package org.apache.ignite.commons.schema;

public class ColumnConfigurationBuilder {
    private final SchemaConfigurationBuilder schemaBuilder;

    private String colName;
    private ColumnType columnType;
    private boolean nullable;
    private Object defValue;

    public ColumnConfigurationBuilder(SchemaConfigurationBuilder schemaBuilder) {
        this.schemaBuilder = schemaBuilder;
    }

    public ColumnConfigurationBuilder withType(ColumnType columnType) {
        this.columnType = columnType;

        return this;
    }

    public ColumnConfigurationBuilder nullable() {
        nullable = true;

        return this;
    }

    public ColumnConfigurationBuilder notNull() {
        nullable = false;

        return this;
    }

    ColumnConfigurationBuilder withName(String colName) {
        this.colName = colName;

        return this;
    }

    public String name() {
        return colName;
    }

    public boolean isNullable() {
        return nullable;
    }

    public ColumnConfigurationBuilder defaultValue(Object defValue) {
        this.defValue = defValue;

        return this;
    }

    public SchemaConfigurationBuilder done() {
        schemaBuilder.addColumn(this);

        return schemaBuilder;
    }
}
