package org.apache.ignite.internal.schema;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.schema.modification.TableModificationBuilderImpl;
import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.SchemaTable;
import org.apache.ignite.schema.TableIndex;
import org.apache.ignite.schema.modification.TableModificationBuilder;

public class SchemaTableImpl implements SchemaTable {

    @Override public List<Column> keyColumns() {
        return null;
    }

    @Override public List<Column> valueColumns() {
        return null;
    }

    @Override public String tableName() {
        return null;
    }

    @Override public String canonicalName() {
        return null;
    }

    @Override public Collection<TableIndex> indices() {
        return null;
    }

    @Override public TableModificationBuilder toBuilder() {
        return new TableModificationBuilderImpl(this);
    }
}
