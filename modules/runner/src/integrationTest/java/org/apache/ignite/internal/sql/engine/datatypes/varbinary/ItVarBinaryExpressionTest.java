package org.apache.ignite.internal.sql.engine.datatypes.varbinary;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Objects;
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

    /** bit-string literal. */
    @Test
    public void testBitStringLiteral() {
        checkQuery("SELECT x'010203'").returns(VarBinary.fromBytes(new byte[]{1, 2, 3}));
    }

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


    /** Concatenation. */
    @Test
    public void testConcat() {
        runSql("INSERT INTO t VALUES (1, x'010203')");
        List<List<Object>> res = runSql("SELECT test_key || x'040506' FROM t");

        assertEquals(1, res.size());
        assertEquals(1, res.get(0).size());
        assertTrue(Objects.deepEquals(new byte[]{1, 2, 3, 4, 5, 6}, res.get(0).get(0)));
    }

    /** Concatenation with dynamic parameter */
    @Test
    public void testConcatWithDynamicParameter() {
        runSql("INSERT INTO t VALUES (1, x'010203')");

        checkQuery("SELECT test_key || ? FROM t WHERE id = 1")
                .withParam(VarBinary.fromBytes(new byte[]{4, 5, 6}))
                .returns(VarBinary.fromBytes(new byte[]{1, 2, 3, 4, 5, 6}))
                .check();
    }

    /** Concatenation of dynamic parameters. */
    @Test
    public void testConcatBetweenDynamicParameters() {
        VarBinary v1 = VarBinary.fromBytes(new byte[]{1, 2, 3});
        VarBinary v2 = VarBinary.fromBytes(new byte[]{4, 5, 6});

        checkQuery("SELECT ? || ?")
                .withParams(v1, v2)
                .returns(VarBinary.fromBytes(new byte[]{1, 2, 3, 4, 5, 6}))
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
