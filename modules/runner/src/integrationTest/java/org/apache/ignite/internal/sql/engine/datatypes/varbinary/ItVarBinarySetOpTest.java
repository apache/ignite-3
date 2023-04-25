package org.apache.ignite.internal.sql.engine.datatypes.varbinary;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.datatypes.DataTypeTestSpecs;
import org.apache.ignite.internal.sql.engine.datatypes.tests.BaseQueryDataTypeTest;
import org.apache.ignite.internal.sql.engine.datatypes.tests.DataTypeTestSpec;

/**
 * Tests for set operators for {@link SqlTypeName#VARBINARY} type.
 */
public class ItVarBinarySetOpTest extends BaseQueryDataTypeTest<VarBinary> {

    /** {@inheritDoc} */
    @Override
    protected DataTypeTestSpec<VarBinary> getTypeSpec() {
        return DataTypeTestSpecs.VARBINARY_TYPE;
    }
}
