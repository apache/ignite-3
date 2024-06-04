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

package org.apache.ignite.internal.sql.engine.type;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/** Tests for {@link IgniteTypeSystem}. */
public class IgniteTypeSystemTest extends BaseIgniteAbstractTest {

    private static final int TIME_PRECISION = 9;
    private static final int STRING_PRECISION = 65536;
    private static final int DECIMAL_PRECISION = 32767;
    private static final int DECIMAL_SCALE = 32767;
    private final IgniteTypeSystem typeSystem = IgniteTypeSystem.INSTANCE;
    
    @Test
    public void getMaxPrecision() {
        assertEquals(STRING_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.CHAR));
        assertEquals(STRING_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.VARCHAR));
        assertEquals(STRING_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.BINARY));
        assertEquals(STRING_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.VARBINARY));

        assertEquals(3, typeSystem.getMaxPrecision(SqlTypeName.TINYINT));
        assertEquals(5, typeSystem.getMaxPrecision(SqlTypeName.SMALLINT));
        assertEquals(10, typeSystem.getMaxPrecision(SqlTypeName.INTEGER));
        assertEquals(19, typeSystem.getMaxPrecision(SqlTypeName.BIGINT));
        assertEquals(8, typeSystem.getMaxPrecision(SqlTypeName.REAL));
        assertEquals(7, typeSystem.getMaxPrecision(SqlTypeName.FLOAT));
        assertEquals(15, typeSystem.getMaxPrecision(SqlTypeName.DOUBLE));
        assertEquals(DECIMAL_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.DECIMAL));

        assertEquals(0, typeSystem.getMaxPrecision(SqlTypeName.DATE));
        assertEquals(TIME_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.TIME));
        assertEquals(TIME_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE));
        assertEquals(TIME_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.TIMESTAMP));
        assertEquals(TIME_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE));
    }
    
    @Test
    public void getDefaultPrecision() {
        assertEquals(STRING_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.CHAR));
        assertEquals(STRING_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.VARCHAR));
        assertEquals(STRING_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.BINARY));
        assertEquals(STRING_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.VARBINARY));
        
        assertEquals(3, typeSystem.getMaxPrecision(SqlTypeName.TINYINT));
        assertEquals(5, typeSystem.getMaxPrecision(SqlTypeName.SMALLINT));
        assertEquals(10, typeSystem.getMaxPrecision(SqlTypeName.INTEGER));
        assertEquals(19, typeSystem.getMaxPrecision(SqlTypeName.BIGINT));
        assertEquals(8, typeSystem.getMaxPrecision(SqlTypeName.REAL));
        assertEquals(7, typeSystem.getMaxPrecision(SqlTypeName.FLOAT));
        assertEquals(15, typeSystem.getMaxPrecision(SqlTypeName.DOUBLE));
        assertEquals(DECIMAL_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.DECIMAL));

        assertEquals(0, typeSystem.getMaxPrecision(SqlTypeName.DATE));
        assertEquals(TIME_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.TIME));
        assertEquals(TIME_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE));
        assertEquals(TIME_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.TIMESTAMP));
        assertEquals(TIME_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE));
    }

    @Test
    public void testGetMaxNumericPrecision() {
        assertEquals(DECIMAL_PRECISION, typeSystem.getMaxNumericPrecision());
    }
    
    @Test
    public void testGetMaxNumericScale() {
        assertEquals(DECIMAL_SCALE, typeSystem.getMaxNumericScale());
    }
}
