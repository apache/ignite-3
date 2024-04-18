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
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.sql.ColumnType;

/** UUID SQL type. */
public final class UuidType extends IgniteCustomType {

    /** A string name of this type: {@code UUID}. **/
    public static final String NAME = "UUID";

    /** Type spec of this type. **/
    public static final IgniteCustomTypeSpec SPEC = new IgniteCustomTypeSpec(NAME, NativeTypes.UUID,
            ColumnType.UUID, UUID.class, IgniteCustomTypeSpec.getCastFunction(UuidType.class, "cast"));

    /** Constructor. */
    public UuidType(boolean nullable) {
        super(SPEC, nullable, PRECISION_NOT_SPECIFIED);
    }

    /** {@inheritDoc} */
    @Override protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append(NAME);
    }

    /** {@inheritDoc} */
    @Override
    public UuidType createWithNullability(boolean nullable) {
        return new UuidType(nullable);
    }

    /**
     * Implementation of a cast function for {@code UUID} data type.
     */
    public static UUID cast(Object value) {
        // It would be better to generate Expression tree that is equivalent to the code below
        // from type checking rules for this type in order to avoid code duplication.
        if (value instanceof String) {
            return UUID.fromString((String) value);
        } else {
            return (UUID) value;
        }
    }
}
