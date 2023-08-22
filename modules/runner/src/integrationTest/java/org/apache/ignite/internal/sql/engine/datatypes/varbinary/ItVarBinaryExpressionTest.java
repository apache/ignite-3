/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine.datatypes.varbinary;

import static org.apache.ignite.internal.sql.engine.util.VarBinary.varBinary;

import java.math.BigDecimal;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.ignite.internal.sql.engine.datatypes.DataTypeTestSpecs;
import org.apache.ignite.internal.sql.engine.datatypes.tests.BaseExpressionDataTypeTest;
import org.apache.ignite.internal.sql.engine.datatypes.tests.DataTypeTestSpec;
import org.apache.ignite.internal.sql.engine.util.VarBinary;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests for expressions for {@link SqlTypeName#VARBINARY} type.
 */
public class ItVarBinaryExpressionTest extends BaseExpressionDataTypeTest<VarBinary> {
    @Override
    protected int nodes() {
        return 1;
    }

    /** Bit-string literal. */
    @Test
    public void testBitStringLiteral() {
        checkQuery("SELECT x'010203'")
                .returns(varBinary(new byte[]{1, 2, 3}))
                .check();
    }

    /** {@code POSITION} expression. */
    @Test
    public void testPositionExpression() {
        checkQuery("SELECT POSITION (x'02' IN x'010203')")
                .returns(2)
                .check();
    }

    /** {@code POSITION} expression. */
    @Test
    public void testPositionExpressionWithDynamicParameter() {
        checkQuery("SELECT POSITION (? IN x'010203')")
                .withParams(varBinary(new byte[]{2}))
                .returns(2)
                .check();

        checkQuery("SELECT POSITION (x'02' IN ?)")
                .withParams(varBinary(new byte[]{1, 2, 3}))
                .returns(2)
                .check();

        checkQuery("SELECT POSITION (? IN ?)")
                .withParams(varBinary(new byte[]{2}), varBinary(new byte[]{1, 2, 3}))
                .returns(2)
                .check();
    }

    /** {@code LENGTH} and {@code OCTET_LENGTH} expression. */
    @Test
    public void testLengthExpression() {
        checkQuery("SELECT LENGTH(x'010203')")
                .returns(3).check();

        checkQuery("SELECT OCTET_LENGTH(x'010203')")
                .returns(3).check();
    }

    /** {@code LENGTH} and {@code OCTET_LENGTH} expression with dynamic params. */
    @Test
    public void testLengthExpressionWithDynamicParameter() {
        checkQuery("SELECT OCTET_LENGTH(?)")
                .withParams(varBinary(new byte[]{1, 2, 3}))
                .returns(3).check();

        checkQuery("SELECT OCTET_LENGTH(?)")
                .withParams(varBinary(new byte[0]))
                .returns(0).check();

        checkQuery("SELECT LENGTH(?)")
                .withParams(varBinary(new byte[]{1, 2, 3}))
                .returns(3).check();

        checkQuery("SELECT LENGTH(?)")
                .withParams(varBinary(new byte[0]))
                .returns(0).check();
    }

    /** Throws correct exception. */
    @Test
    public void testErroneousParamToLegth() {
        IgniteTestUtils.assertThrowsWithCause(() -> checkQuery("SELECT LENGTH(?)")
                .withParams(new BigDecimal(1)).check(), SqlValidatorException.class,
                "Values passed to LENGTH operator must have compatible types");
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
                .returns((varBinary(new byte[]{(byte) 0xfff, (byte) 0xff})))
                .check();

        checkQuery("SELECT CAST(X'ffffff' AS VARBINARY(100))")
                .returns((varBinary(new byte[]{(byte) 0xfff, (byte) 0xff, (byte) 0xff})))
                .check();

        checkQuery("SELECT CAST(X'ffffff' AS VARBINARY)")
                .returns((varBinary(new byte[]{(byte) 0xfff, (byte) 0xff, (byte) 0xff})))
                .check();
    }

    /**
     * {@code CAST} to {@code VARBINARY} with different length with dynamic parameters.
     */
    @Test
    public void testCastToDifferentLengthsWithDynamicParameters() {
        byte[] param = {1, 2, 3};
        byte[] result = {1, 2};

        checkQuery("SELECT CAST(? AS VARBINARY(2))")
                .withParam(param)
                .returns(varBinary(result))
                .check();

        checkQuery("SELECT CAST(? AS VARBINARY(100))")
                .withParam(param)
                .returns(varBinary(param))
                .check();

        checkQuery("SELECT CAST(? AS VARBINARY)")
                .withParam(param)
                .returns(varBinary(param))
                .check();
    }


    /** Concatenation. */
    @Test
    public void testConcat() {
        runSql("INSERT INTO t VALUES (1, x'010203')");

        checkQuery("SELECT test_key || x'040506' FROM t")
                .returns(varBinary(new byte[]{1, 2, 3, 4, 5, 6}))
                .check();
    }

    /** Concatenation with dynamic parameter. */
    @Test
    public void testConcatWithDynamicParameter() {
        runSql("INSERT INTO t VALUES (1, x'010203')");

        checkQuery("SELECT test_key || ? FROM t WHERE id = 1")
                .withParam(varBinary(new byte[]{4, 5, 6}))
                .returns(varBinary(new byte[]{1, 2, 3, 4, 5, 6}))
                .check();
    }

    /** Concatenation of dynamic parameters. */
    @Test
    public void testConcatBetweenDynamicParameters() {
        VarBinary v1 = varBinary(new byte[]{1, 2, 3});
        VarBinary v2 = varBinary(new byte[]{4, 5, 6});

        checkQuery("SELECT ? || ?")
                .withParams(v1, v2)
                .returns(varBinary(new byte[]{1, 2, 3, 4, 5, 6}))
                .check();
    }

    /** {@code LIKE} operator. */
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
