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

package org.apache.ignite.internal.sql.engine.rex;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomType;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.jetbrains.annotations.Nullable;

/**
 * {@link RexBuilder} that provides support for {@link IgniteCustomType custom data types}.
 */
public class IgniteRexBuilder extends RexBuilder {
    public static final IgniteRexBuilder INSTANCE = new IgniteRexBuilder(IgniteTypeFactory.INSTANCE);

    /**
     * Creates a RexBuilder.
     *
     * @param typeFactory Type factory
     */
    private IgniteRexBuilder(RelDataTypeFactory typeFactory) {
        super(typeFactory);
    }

    /** {@inheritDoc} **/
    @Override
    public RexNode makeLiteral(@Nullable Object value, RelDataType type, boolean allowCast, boolean trim) {
        // We need to override this method because otherwise
        // default implementation will call RexBuilder::guessType(@Nullable Object value)
        // for a custom data type which will then raise the following assertion:
        //
        //  throw new AssertionError("unknown type " + value.getClass());
        //
        if (value != null && type instanceof IgniteCustomType) {
            // IgniteCustomType: Not comparable types are not supported.
            assert value instanceof Comparable : "Not comparable IgniteCustomType:" + type + ". value: " + value;
            return makeLiteral((Comparable<?>) value, type, type.getSqlTypeName());
        } else {
            return super.makeLiteral(value, type, allowCast, trim);
        }
    }
}
