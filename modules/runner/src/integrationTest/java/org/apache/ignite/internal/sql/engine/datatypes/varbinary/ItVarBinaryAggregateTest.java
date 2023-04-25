package org.apache.ignite.internal.sql.engine.datatypes.varbinary;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.datatypes.DataTypeTestSpecs;
import org.apache.ignite.internal.sql.engine.datatypes.tests.BaseAggregateDataTypeTest;
import org.apache.ignite.internal.sql.engine.datatypes.tests.DataTypeTestSpec;

/**
 * Tests for aggregates for {@link SqlTypeName#VARBINARY} type.
 */
public class ItVarBinaryAggregateTest extends BaseAggregateDataTypeTest<VarBinary> {

    /** {@inheritDoc} */
    @Override
    protected DataTypeTestSpec<VarBinary> getTypeSpec() {
        return DataTypeTestSpecs.VARBINARY_TYPE;
    }
}
