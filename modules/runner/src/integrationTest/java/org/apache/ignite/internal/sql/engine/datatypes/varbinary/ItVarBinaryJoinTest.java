package org.apache.ignite.internal.sql.engine.datatypes.varbinary;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.datatypes.DataTypeTestSpecs;
import org.apache.ignite.internal.sql.engine.datatypes.tests.BaseJoinDataTypeTest;
import org.apache.ignite.internal.sql.engine.datatypes.tests.DataTypeTestSpec;

/**
 * Tests for {@code JOIN} operator for {@link SqlTypeName#VARBINARY} type.
 */
public class ItVarBinaryJoinTest extends BaseJoinDataTypeTest<VarBinary> {

    /** {@inheritDoc} */
    @Override
    protected DataTypeTestSpec<VarBinary> getTypeSpec() {
        return DataTypeTestSpecs.VARBINARY_TYPE;
    }
}
