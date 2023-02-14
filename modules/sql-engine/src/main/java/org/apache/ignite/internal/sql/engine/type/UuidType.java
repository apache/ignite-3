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

import java.util.UUID;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.sql.ColumnType;

/** UUID SQL type. */
public final class UuidType extends IgniteCustomType<UUID> {

    /** A string name of this type: {@code UUID}. **/
    public static final String NAME = "UUID";

    /** The storage type. **/
    public static final Class<UUID> JAVA_TYPE = UUID.class;

    /** Constructor. */
    public UuidType(boolean nullable) {
        super(JAVA_TYPE, nullable, PRECISION_NOT_SPECIFIED);
    }

    /** {@inheritDoc} */
    @Override protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append(NAME);
    }

    /** {@inheritDoc} */
    @Override
    public String getCustomTypeName() {
        return NAME;
    }

    /** {@inheritDoc} */
    @Override
    public NativeType nativeType() {
        return NativeTypes.UUID;
    }

    /** {@inheritDoc} */
    @Override
    public ColumnType columnType() {
        return ColumnType.UUID;
    }

    /** {@inheritDoc} */
    @Override
    public UuidType createWithNullability(boolean nullable) {
        return new UuidType(nullable);
    }
}
