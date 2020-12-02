package org.apache.ignite.schema;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ignite.schema.builder.SchemaModificationBuilder;

public interface TableSchema {
    List<Column> keyColumns();

    List<Column> valueColumns();

    String name();

    Map<String, String> aliases();

    Collection<TableIndex> indices();

    SchemaModificationBuilder toBuilder();
}
