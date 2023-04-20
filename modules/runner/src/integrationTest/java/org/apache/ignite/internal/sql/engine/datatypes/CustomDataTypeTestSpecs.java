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

package org.apache.ignite.internal.sql.engine.datatypes;

import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.util.UUID;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.datatypes.tests.CustomDataTypeTestSpec;
import org.apache.ignite.internal.sql.engine.datatypes.tests.TestDataSamples;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.type.UuidType;
import org.apache.ignite.sql.ColumnType;

/**
 * Defines {@link CustomDataTypeTestSpec test specs} for data types.
 */
public final class CustomDataTypeTestSpecs {

    /**
     * Test spec for {@link UuidType UUID} data type.
     */
    public static final CustomDataTypeTestSpec<UUID> UUID_TYPE = new CustomDataTypeTestSpec<>(
            ColumnType.UUID, UuidType.NAME, UUID.class, new UUID[]{new UUID(1, 1), new UUID(2, 1), new UUID(3, 1)}) {

        @Override
        public boolean hasLiterals() {
            return false;
        }

        @Override
        public String toLiteral(UUID value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toValueExpr(UUID value) {
            return format("'{}'::UUID", value);
        }

        @Override
        public String toStringValue(UUID value) {
            return value.toString();
        }

        @Override
        public TestDataSamples<UUID> createSamples(IgniteTypeFactory typeFactory) {
            TestDataSamples.Builder<UUID> samples = TestDataSamples.builder();

            samples.add(values, SqlTypeName.VARCHAR, String::valueOf);
            samples.add(values, SqlTypeName.CHAR, String::valueOf);

            return samples.build();
        }
    };

    private CustomDataTypeTestSpecs() {

    }
}
