package org.apache.ignite.commons.schema;

import org.junit.jupiter.api.Test;

public class SchemaConfigurationTest {

    @Test
    public void testBasicSchema() {
        final SchemaConfigurationBuilder builder = SchemaConfigurationBuilder.create();

        builder.withName("TestTabke")
            .addKeyColumn("id").withType(ColumnType.INT64).done()
            .addKeyColumn("affId").withType(ColumnType.INT32).affinityColumn().done()
            .addKeyColumn("labal").withType(ColumnType.string().lengthOf(2)).defaultValue("GG").done()

            .addValueColumn("name").withType(ColumnType.string()).notNull().done()
            .addValueColumn("data").withType(ColumnType.blob().lengthOf(255)).nullable().done()

            .addIndex("idx_1")
            .addIndexColumn("id").desc().done()
            .addIndexColumn("name").asc().done()
            .inlineSize(42)
            .done()

            .addAlias("colAlias", "name")
            .build();

    }

}
