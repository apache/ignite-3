package org.apache.ignite.internal.sql.engine.datatypes.varbinary;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.datatypes.DataTypeTestSpecs;
import org.apache.ignite.internal.sql.engine.datatypes.tests.BaseIndexDataTypeTest;
import org.apache.ignite.internal.sql.engine.datatypes.tests.DataTypeTestSpec;

/**
 * Tests for queries that use indexes with {@link SqlTypeName#VARBINARY} type.
 */
public class ItVarBinaryIndexTest extends BaseIndexDataTypeTest<VarBinary> {

    /** {@inheritDoc} */
    @Override
    protected DataTypeTestSpec<VarBinary> getTypeSpec() {
        return DataTypeTestSpecs.VARBINARY_TYPE;
    }
}
