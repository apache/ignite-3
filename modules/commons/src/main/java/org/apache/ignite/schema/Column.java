package org.apache.ignite.schema;

public interface Column {
    ColumnType type();

    String name();

    boolean nullable();

    Object defaultValue();

    boolean isKeyColumn();

    boolean isAffinityColumn();
}
