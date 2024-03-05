package org.apache.ignite.internal.catalog.descriptors;

/** Descriptor for the index system view. */
public class CatalogIndexViewDescriptor {
    /** Index ID. */
    private final int id;

    /** Index name. */
    private final String name;

    /** index type. */
    private final String type;

    /** ID of the table the index is created for. */
    private final int tableId;

    /** Name of the table the index is created for. */
    private final String tableName;

    /** ID of the schema where the index is created. */
    private final int schemaId;

    /** Name of the schema where the index is created. */
    private final String schemaName;

    /** Unique constraint flag. */
    private final boolean unique;

    /** Columns used in the index separated by comma. For sorted index format is "{column_name} {collation}". */
    private final String columnsString;

    /** Status of the index. */
    private final String status;

    public CatalogIndexViewDescriptor(
            int id,
            String name,
            String type,
            int tableId,
            String tableName,
            int schemaId,
            String schemaName,
            boolean unique,
            String columnsString,
            String status
    ) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.tableId = tableId;
        this.tableName = tableName;
        this.schemaId = schemaId;
        this.schemaName = schemaName;
        this.unique = unique;
        this.columnsString = columnsString;
        this.status = status;
    }

    /** Returns ID of the index. */
    public int id() {
        return id;
    }

    /** Returns name of the index. */
    public String name() {
        return name;
    }

    /** Returns list of columns used in the index. For sorted index format is "{column_name} {collation}". */
    public String columnsString() {
        return columnsString;
    }

    /** Returns type of the index. */
    public String type() {
        return type;
    }

    /** Returns ID of the table. */
    public int tableId() {
        return tableId;
    }

    /** Returns unique constraint flag. */
    public boolean unique() {
        return unique;
    }

    /** Returns name of the table. */
    public String tableName() {
        return tableName;
    }

    /** Returns ID of the schema. */
    public int schemaId() {
        return schemaId;
    }

    /** Returns name of the schema. */
    public String schemaName() {
        return schemaName;
    }

    /** Returns status of the index. */
    public String status() {
        return status;
    }
}
