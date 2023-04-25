package org.apache.ignite.internal.sql.engine.datatypes.varbinary;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.datatypes.DataTypeTestSpecs;
import org.apache.ignite.internal.sql.engine.datatypes.tests.BaseQueryDataTypeTest;
import org.apache.ignite.internal.sql.engine.datatypes.tests.DataTypeTestSpec;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@code SELECT} operator for {@link SqlTypeName#VARBINARY} type.
 */
public class ItVarBinaryQueryTest extends BaseQueryDataTypeTest<VarBinary> {

    @Test
    @Override
    public void testInWithDynamicParamsCondition() {
        super.testInWithDynamicParamsCondition();
    }

    /** {@inheritDoc} */
    @Override
    protected DataTypeTestSpec<VarBinary> getTypeSpec() {
        return DataTypeTestSpecs.VARBINARY_TYPE;
    }
}
