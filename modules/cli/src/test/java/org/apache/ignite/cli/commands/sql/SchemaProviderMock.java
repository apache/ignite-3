package org.apache.ignite.cli.commands.sql;

import java.util.Map;
import java.util.Set;
import org.apache.ignite.cli.sql.SchemaProvider;
import org.apache.ignite.cli.sql.SqlSchema;

class SchemaProviderMock implements SchemaProvider {
    @Override
    public SqlSchema getSchema() {
        return new SqlSchema(Map.of("PUBLIC", Map.of("PERSON", Set.of("ID", "NAME", "SALARY"))));
    }
}
