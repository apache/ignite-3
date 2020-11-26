package org.apache.ignite.commons.schema;

public class KeyColumnConfigurationBuilder extends ColumnConfigurationBuilder {
    private boolean isAffinityColumn = false;

    public KeyColumnConfigurationBuilder(SchemaConfigurationBuilder schemaBuilder) {
        super(schemaBuilder);
    }

    public KeyColumnConfigurationBuilder affinityColumn() {
        isAffinityColumn = true;

        return this;
    }

    public boolean isAffinityColumn() {
        return isAffinityColumn;
    }

    @Override public KeyColumnConfigurationBuilder withName(String colName) {
        super.withName(colName);

        return this;
    }

    @Override public KeyColumnConfigurationBuilder withType(ColumnType columnType) {
        super.withType(columnType);

        return this;
    }

    @Override public KeyColumnConfigurationBuilder nullable() {
        super.nullable();

        return this;
    }

    @Override public KeyColumnConfigurationBuilder notNull() {
        super.notNull();

        return this;
    }

    @Override public KeyColumnConfigurationBuilder defaultValue(Object defValue) {
        super.defaultValue(defValue);

        return this;
    }
}
