package org.apache.ignite.internal.sql.engine.planner;

import java.util.BitSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.jetbrains.annotations.Nullable;

public class TestTable extends org.apache.ignite.internal.sql.engine.planner.util.TestTable {

    private final SchemaDescriptor schemaDesc;

    public TestTable(RelDataType rowType, SchemaDescriptor schemaDescriptor) {
        super(rowType);

        schemaDesc = schemaDescriptor;
    }

    @Override
    public IgniteDistribution distribution() {
        return IgniteDistributions.broadcast();
    }

    @Override
    public <RowT> RowT toRow(ExecutionContext<RowT> ectx, BinaryRow binaryRow, RowFactory<RowT> factory,
            @Nullable BitSet requiredColumns) {
        TableDescriptor desc = descriptor();
        Row tableRow = new Row(schemaDesc, binaryRow);

        RowT row = factory.create();
        RowHandler<RowT> handler = factory.handler();

        for (int i = 0; i < desc.columnsCount(); i++) {
            handler.set(i, row, TypeUtils.toInternal(tableRow.value(desc.columnDescriptor(i).physicalIndex())));
        }

        return row;
    }
}
