package org.apache.ignite.internal.sql.engine.datatypes.varbinary;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.datatypes.DataTypeTestSpecs;
import org.apache.ignite.internal.sql.engine.datatypes.tests.BaseExpressionDataTypeTest;
import org.apache.ignite.internal.sql.engine.datatypes.tests.DataTypeTestSpec;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests for expressions for {@link SqlTypeName#VARBINARY} type.
 */
public class ItVarBinaryExpressionTest extends BaseExpressionDataTypeTest<VarBinary> {

    /**
     * {@code CAST} to {@code VARBINARY} with different length.
     */
    @Test
    public void testCastToDifferentLengths() {
        checkQuery("SELECT CAST('123' AS VARBINARY(2))")
                .returns(VarBinary.fromUtf8String("12"))
                .check();

        checkQuery("SELECT CAST('123' AS VARBINARY(100))")
                .returns((VarBinary.fromUtf8String("123")))
                .check();

        checkQuery("SELECT CAST('123' AS VARBINARY)")
                .returns((VarBinary.fromUtf8String("123")))
                .check();

        checkQuery("SELECT CAST(X'ffffff' AS VARBINARY(2))")
                .returns((VarBinary.fromBytes(new byte[]{(byte) 0xfff, (byte) 0xff})))
                .check();

        checkQuery("SELECT CAST(X'ffffff' AS VARBINARY(100))")
                .returns((VarBinary.fromBytes(new byte[]{(byte) 0xfff, (byte) 0xff, (byte) 0xff})))
                .check();

        checkQuery("SELECT CAST(X'ffffff' AS VARBINARY)")
                .returns((VarBinary.fromBytes(new byte[]{(byte) 0xfff, (byte) 0xff, (byte) 0xff})))
                .check();
    }

    /**
     * {@code CAST} to {@code VARBINARY} with different length with dynamic parameters.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19353")
    @Test
    public void testCastToDifferentLengthsWithDynamicParameters() {
        byte[] param = {1, 2, 3};
        byte[] result = {1, 2};

        checkQuery("SELECT CAST(? AS VARBINARY(2))")
                .withParam(param)
                .returns(VarBinary.fromBytes(result))
                .check();

        checkQuery("SELECT CAST(? AS VARBINARY(100))")
                .withParam(param)
                .returns(VarBinary.fromBytes(param))
                .check();

        checkQuery("SELECT CAST(? AS VARBINARY)")
                .withParam(param)
                .returns(VarBinary.fromBytes(param))
                .check();
    }

    /** {@code LIKE} operator */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18166")
    @Test
    public void testLike() {
        assertQuery("SELECT 'aaaaa'::VARBINARY LIKE 'aa'").check();
    }

    /** {@inheritDoc} */
    @Override
    protected DataTypeTestSpec<VarBinary> getTypeSpec() {
        return DataTypeTestSpecs.VARBINARY_TYPE;
    }
}
