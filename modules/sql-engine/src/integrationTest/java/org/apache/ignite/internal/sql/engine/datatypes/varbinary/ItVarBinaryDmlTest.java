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

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.datatypes.DataTypeTestSpecs;
import org.apache.ignite.internal.sql.engine.datatypes.tests.BaseDmlDataTypeTest;
import org.apache.ignite.internal.sql.engine.datatypes.tests.DataTypeTestSpec;
import org.apache.ignite.internal.sql.engine.util.VarBinary;
import org.junit.jupiter.api.Test;

/**
 * Tests for DML operations for {@link SqlTypeName#VARBINARY} type.
 */
public class ItVarBinaryDmlTest extends BaseDmlDataTypeTest<VarBinary> {

    /** {@code INSERT} into a column with DEFAULT constraint. */
    @Test
    public void testDefault() {
        runSql("CREATE TABLE t_def (id INT PRIMARY KEY, test_key VARBINARY DEFAULT $0)");
        runSql("INSERT INTO t_def (id) VALUES (0)");

        byte[] value = values.get(0).get();
        checkQuery("SELECT test_key FROM t_def WHERE id=0")
                .returns(value)
                .check();
    }

    /** {@code INSERT} an empty varbinary. */
    @Test
    public void testEmptyVarBinary() {
        byte[] value = new byte[0];

        runSql("INSERT INTO t VALUES (1, ?)", value);

        checkQuery("SELECT test_key FROM t WHERE id = 1")
                .returns(value)
                .check();
    }

    /** {@code INSERT} a HEX literal. */
    @Test
    public void testInsertHexLiteral() {
        byte[] value = {(byte) 0xAA, (byte) 0xBB, (byte) 0xCC};

        runSql("INSERT INTO t VALUES (1, x'AABBCC')");

        checkQuery("SELECT test_key FROM t WHERE id = 1")
                .returns(value)
                .check();
    }

    /** {@inheritDoc} */
    @Override
    protected DataTypeTestSpec<VarBinary> getTypeSpec() {
        return DataTypeTestSpecs.VARBINARY_TYPE;
    }
}
