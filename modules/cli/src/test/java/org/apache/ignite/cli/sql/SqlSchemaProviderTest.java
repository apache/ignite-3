package org.apache.ignite.cli.sql;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class SqlSchemaProviderTest {
    private static MetadataSupplier supplier;

    @BeforeAll
    public static void setup() throws SQLException {
        DatabaseMetaData meta = mock(DatabaseMetaData.class);

        when(meta.getTables(null, null, null, null)).thenReturn(mock(ResultSet.class));
        when(meta.getTables(any(), any(), any(), any())).thenReturn(mock(ResultSet.class));


        supplier = () -> meta;
    }

    @Test
    public void testProviderWithoutTimeout() {
        SqlSchemaProvider provider = new SqlSchemaProvider(supplier, 0);
        Assertions.assertNotEquals(provider.getSchema(), provider.getSchema());
    }

    @Test
    public void testProviderWith1secTimeout() throws InterruptedException {
        SqlSchemaProvider provider = new SqlSchemaProvider(supplier, 1);
        SqlSchema schema = provider.getSchema();
        Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        Assertions.assertNotEquals(schema, provider.getSchema());
    }
}
