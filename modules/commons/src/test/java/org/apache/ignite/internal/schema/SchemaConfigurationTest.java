package org.apache.ignite.internal.schema;

import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.SchemaBuilder;
import org.junit.jupiter.api.Test;

public class SchemaConfigurationTest {

    @Test
    public void testBasicSchema() {
        final SchemaBuilder builder = SchemaConfigurationBuilder.create();

        builder.withName("TestTable")
            .keyColumns()
            .addColumn("id").withType(ColumnType.INT64).done()
            .addColumn("affId").withType(ColumnType.INT32).affinityColumn().done()
            .addColumn("labal").withType(ColumnType.stringOf(2)).defaultValue("AI").done()
            .done()

            .valueColumns()
            .addColumn("name").withType(ColumnType.string()).notNull().done()
            .addColumn("data").withType(ColumnType.blobOf(255)).nullable().done()
            .done()

            .addIndex("idx_1")
            .addIndexColumn("id").desc().done()
            .addIndexColumn("name").asc().done()
            .inlineSize(42)
            .done()

            .addAlias("colAlias", "name")
            .build();
    }
}
