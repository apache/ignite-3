package org.apache.ignite.internal.schema;

import org.apache.ignite.internal.schema.builder.SchemaTableBuilderImpl;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.builder.SchemaBuilders;
import org.apache.ignite.schema.builder.SchemaTableBuilder;
import org.junit.jupiter.api.Test;

public class SchemaConfigurationTest {

    @Test
    public void testInitialSchema() {
        //TODO: Do we need separate 'Schema builder' or left 'schema' name as kind of 'namespace'.
        final SchemaTableBuilder builder = SchemaTableBuilderImpl.tableBuilder("PUBLIC", "table1");

        builder
            .columns()
            // Declaring columns in user order.
            .addColumn("id").withType(ColumnType.INT64).done()
            .addColumn("label").withType(ColumnType.stringOf(2)).withDefaultValue("AI").done()
            .addColumn("name").withType(ColumnType.string()).asNotNull().done()
            .addColumn("data").withType(ColumnType.blobOf(255)).asNullable().done()
            .addColumn("affId").withType(ColumnType.INT32).done()
            .done()

            // PK index type can't be changed as highly coupled core implementation.
            .pk()
            .withColumns("id", "affId", "label") // Declare index column in order.
            .withAffinityColumns("affId") // Optional affinity declaration. If not set, all columns will be affinity cols.
            //TODO: As we have affinity columns here,
            //TODO: do we want to add affinity function config here???
            .done()

            // 'withIndex' single entry point allows extended index support.
            // E.g. we may want to support GEO indices later with some plugin.
            .withindex(
                SchemaBuilders.sortedIndex("idx_1_sorted")
                    .addIndexColumn("id").desc().done()
                    .addIndexColumn("name").asc().done()
                    .withInlineSize(42)
                    .build()
            )

            .withindex(
                SchemaBuilders.partialIndex("idx_2_partial")
                    .addIndexColumn("id").desc().done()
                    .addIndexColumn("name").asc().done()
                    .withExpression("id > 0")
                    .withInlineSize(42)
                    .build()
            )

            .withindex(
                SchemaBuilders.hashIndex("idx_3_hash")
                    .withColumns("id", "affId")
                    .build()
            )

            .build();
    }

    @Test
    public void testSchemaModification() {

    }
}
