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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link IgniteTypeFactory}.
 */
public class IgniteTypeFactorySelfTest extends BaseIgniteAbstractTest {

    @Test
    public void createUuidProducesIgniteUuid() {
        IgniteTypeFactory typeFactory = Commons.typeFactory();

        RelDataType uuid = typeFactory.createSqlType(SqlTypeName.UUID);
        assertInstanceOf(UuidType.class, uuid);
        assertFalse(uuid.isNullable());

        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        assertEquals(varchar.isNullable(), uuid.isNullable(), "default nullability");

        // Use customType method
        assertEquals(typeFactory.createCustomType(UuidType.NAME), uuid);

        // Nullable
        RelDataType nullable = typeFactory.createTypeWithNullability(uuid, true);
        assertTrue(nullable.isNullable());
        assertNotEquals(uuid, nullable, uuid.getFullTypeString() + " | " + nullable.getFullTypeString());

        // Not Nullable
        RelDataType notNullable = typeFactory.createTypeWithNullability(uuid, false);
        assertFalse(notNullable.isNullable());
        assertEquals(uuid, notNullable, uuid.getFullTypeString() + " | " + notNullable.getFullTypeString());
    }
}
