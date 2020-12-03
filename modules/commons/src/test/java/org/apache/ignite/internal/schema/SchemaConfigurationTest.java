package org.apache.ignite.internal.schema;

import org.apache.ignite.internal.schema.builder.SchemaTableBuilderImpl;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.builder.SchemaTableBuilder;
import org.junit.jupiter.api.Test;

public class SchemaConfigurationTest {

    @Test
    public void testBasicSchema() {

        //TODO: split to SchemaBuilder and TableBuilder?
        final SchemaTableBuilder builder = SchemaTableBuilderImpl.tableBuilder("PUBLIC", "table1");

        builder
            .columns()
            // Declaring columns in user order.
            .addColumn("id").withType(ColumnType.INT64).done()
            .addColumn("label").withType(ColumnType.stringOf(2)).defaultValue("AI").done()
            .addColumn("name").withType(ColumnType.string()).notNull().done()
            .addColumn("data").withType(ColumnType.blobOf(255)).nullable().done()
            .addColumn("affId").withType(ColumnType.INT32).done()
            .done()

            .pk()
            // Declare index column in order.
            .addIndexColumn("id").done()
            .addIndexColumn("affId").done()
            .addIndexColumn("label").done()
            .done()

            // Optional affinity declaration. If not set,
            .affinityColumns("affId")


            .addSortedIndex("idx_1_sorted")
            .addIndexColumn("id").desc().done()
            .addIndexColumn("name").asc().done()
            .inlineSize(42)
            .done()

            .addPartialIndex("idx_2_partial")
            .addIndexColumn("id").desc().done()
            .addIndexColumn("name").asc().done()
            .expr("id > 0")
            .inlineSize(42)
            .done()

            .addHashIndex("idx_3_hash")
            .columns("id", "affId")
            .done()

            .build();
    }
}
