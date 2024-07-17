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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import com.google.common.io.BaseEncoding;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.datatypes.tests.DataTypeTestSpec;
import org.apache.ignite.internal.sql.engine.datatypes.tests.TestDataSamples;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.type.UuidType;
import org.apache.ignite.internal.sql.engine.util.VarBinary;
import org.apache.ignite.sql.ColumnType;

/**
 * Defines {@link DataTypeTestSpec test specs} for data types.
 */
public final class DataTypeTestSpecs {

    /**
     * Test type spec for {@link UuidType UUID} data type.
     */
    public static final DataTypeTestSpec<UUID> UUID_TYPE = new DataTypeTestSpec<>(
            ColumnType.UUID, UuidType.NAME, UUID.class) {

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
        public UUID wrapIfNecessary(Object storageValue) {
            return (UUID) storageValue;
        }

        @Override
        public TestDataSamples<UUID> createSamples(IgniteTypeFactory typeFactory) {
            List<UUID> values = List.of(new UUID(1, 1), new UUID(2, 1), new UUID(3, 1));
            TestDataSamples.Builder<UUID> samples = TestDataSamples.builder();

            samples.add(values, SqlTypeName.VARCHAR, String::valueOf);

            return samples.build();
        }
    };

    /**
     * Test type spec for {@link SqlTypeName#VARBINARY} data type.
     */
    public static final DataTypeTestSpec<VarBinary> VARBINARY_TYPE = new DataTypeTestSpec<>(
            ColumnType.BYTE_ARRAY, "VARBINARY", VarBinary.class) {

        /** {@inheritDoc} */
        @Override
        public boolean hasLiterals() {
            return true;
        }

        /** {@inheritDoc} */
        @Override
        public String toLiteral(VarBinary value) {
            return format("x'{}'", BaseEncoding.base16().encode(value.get()));
        }

        @Override
        public String toValueExpr(VarBinary value) {
            return toLiteral(value);
        }

        /** {@inheritDoc} */
        @Override
        public String toStringValue(VarBinary value) {
            return value.asString(StandardCharsets.UTF_8);
        }

        /** {@inheritDoc} */
        @Override
        public VarBinary wrapIfNecessary(Object storageValue) {
            return VarBinary.varBinary((byte[]) storageValue);
        }

        /** {@inheritDoc} */
        @Override
        public byte[] unwrapIfNecessary(VarBinary value) {
            if (value == null) {
                return null;
            }

            return value.get();
        }

        /** {@inheritDoc} */
        @Override
        public TestDataSamples<VarBinary> createSamples(IgniteTypeFactory typeFactory) {
            List<VarBinary> values = List.of(
                    VarBinary.fromBytes(new byte[] {(byte) 1}),
                    VarBinary.fromBytes(new byte[] {(byte) 2}),
                    VarBinary.fromBytes(new byte[] {(byte) 3}));

            TestDataSamples.Builder<VarBinary> samples = TestDataSamples.builder();

            samples.add(values, SqlTypeName.BINARY, b -> b.asString(StandardCharsets.UTF_8).getBytes(StandardCharsets.UTF_8));

            return samples.build();
        }
    };

    private DataTypeTestSpecs() {

    }
}
