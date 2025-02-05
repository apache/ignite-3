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

package org.apache.ignite.internal.sql.engine.planner.datatypes.utils;

import org.apache.ignite.internal.type.NativeType;

/**
 * Enumerates possible binary type pairs for test purposes.
 */
public enum BinaryPair implements TypePair {
    VARBINARY1_VARBINARY2(Types.VARBINARY_1, Types.VARBINARY_2),
    VARBINARY2_VARBINARY1(Types.VARBINARY_2, Types.VARBINARY_1),
    VARBINARY1_VARBINARY1(Types.VARBINARY_1, Types.VARBINARY_1),
    VARBINARY_VARBINARY2(Types.VARBINARY, Types.VARBINARY_2);

    private final NativeType first;
    private final NativeType second;

    BinaryPair(NativeType first, NativeType second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public NativeType first() {
        return first;
    }

    @Override
    public NativeType second() {
        return second;
    }
}
